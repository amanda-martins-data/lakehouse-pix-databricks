"""Transformações Bronze -> Silver -> Gold, como funções puras de DataFrame.

As funções não leem nem gravam tabelas: recebem e devolvem DataFrames.
Assim os notebooks cuidam só de I/O (Auto Loader, MERGE), e a lógica
roda igual no Databricks e nos testes locais com PySpark.
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from .schema import (CAMPOS_QUANTIDADE, CAMPOS_VALOR, CHAVE, MUNICIPIO_NAO_INFORMADO,
                     PRIMEIRO_MES, RENOMEAR)

_RE_EXTRAIDO = r"_(\d{8}T\d{6}Z)\.jsonl$"


def extraido_em_do_caminho(caminho: Column) -> Column:
    """Data da extração, lida do nome do arquivo gravado por extract.nome_arquivo."""
    return F.to_timestamp(F.regexp_extract(caminho, _RE_EXTRAIDO, 1), "yyyyMMdd'T'HHmmss'Z'")


def mes_parcial(ano_mes: Column, extraido_em: Column) -> Column:
    """Um mês é parcial se ainda não tinha acabado quando foi extraído,
    ou se é novembro de 2020, quando o Pix começou no dia 16."""
    mes_da_extracao = F.date_format(extraido_em, "yyyyMM").cast("long")
    return (ano_mes >= mes_da_extracao) | (ano_mes == F.lit(PRIMEIRO_MES))


def padronizar(bronze: DataFrame) -> DataFrame:
    """Renomeia, tipa e cria a chave do município.

    Espera as colunas da fonte mais _arquivo e _extraido_em (da Bronze).
    """
    df = bronze.select(
        *[F.col(origem).alias(destino) for origem, destino in RENOMEAR.items()],
        F.col("_arquivo"), F.col("_extraido_em"),
    )
    for c in CAMPOS_VALOR:
        df = df.withColumn(c, F.col(c).cast("decimal(20,2)"))
    for c in CAMPOS_QUANTIDADE:
        df = df.withColumn(c, F.col(c).cast("long"))
    return (df
            .withColumn("ano_mes", F.col("ano_mes").cast("int"))
            .withColumn("municipio_ibge", F.col("municipio_ibge").cast("int"))
            .withColumn("uf_ibge", F.col("uf_ibge").cast("int"))
            # Linha "N/D": código nulo vira sentinela, senão o MERGE nunca casa (decisions/0003).
            .withColumn("municipio_chave", F.coalesce(F.col("municipio_ibge"), F.lit(MUNICIPIO_NAO_INFORMADO)))
            .withColumn("municipio_informado", F.col("municipio_ibge").isNotNull())
            .withColumn("mes_parcial", mes_parcial(F.col("ano_mes"), F.col("_extraido_em"))))


def _metricas() -> Column:
    """Todas as métricas num array, para testar "alguma é negativa/nula" numa expressão só."""
    return F.array(*[F.col(c).cast("decimal(20,2)") for c in CAMPOS_VALOR + CAMPOS_QUANTIDADE])


def validar(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Separa linhas válidas das que vão para quarentena, com o motivo."""
    motivos = [
        (F.col("ano_mes").isNull() | (F.col("ano_mes") < PRIMEIRO_MES) | ((F.col("ano_mes") % 100) > 12),
         "ano_mes fora do intervalo"),
        (F.col("municipio_informado") & ((F.col("municipio_ibge") < 1_000_000) | (F.col("municipio_ibge") > 9_999_999)),
         "codigo IBGE sem 7 digitos"),
        (F.col("municipio_informado") & F.col("uf_ibge").isNull(), "municipio sem UF"),
        (F.exists(_metricas(), lambda x: x < 0), "valor ou quantidade negativa"),
        (F.exists(_metricas(), lambda x: x.isNull()), "metrica nula"),
    ]
    motivo = F.concat_ws("; ", *[F.when(cond, F.lit(txt)) for cond, txt in motivos])
    marcado = df.withColumn("_motivo", motivo)
    validos = marcado.filter(F.col("_motivo") == "").drop("_motivo")
    quarentena = marcado.filter(F.col("_motivo") != "")
    return validos, quarentena


def ultima_extracao(df: DataFrame) -> DataFrame:
    """Uma linha por (ano_mes, municipio): a da extração mais recente.

    O mesmo mês é extraído de novo enquanto é parcial (e nos reprocessamentos),
    então a Bronze guarda várias versões da mesma chave.
    """
    w = Window.partitionBy(*CHAVE).orderBy(F.col("_extraido_em").desc(), F.col("_arquivo").desc())
    return df.withColumn("_ordem", F.row_number().over(w)).filter("_ordem = 1").drop("_ordem")


def silver(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    validos, quarentena = validar(padronizar(bronze))
    return ultima_extracao(validos), quarentena


def gold_municipio_mes(silver_df: DataFrame) -> DataFrame:
    """Métricas por município e mês, com crescimento contra o mesmo mês do ano anterior."""
    df = (silver_df
          .withColumn("valor_pago", F.col("vl_pagador_pf") + F.col("vl_pagador_pj"))
          .withColumn("transacoes_pagas", F.col("qt_pagador_pf") + F.col("qt_pagador_pj"))
          .withColumn("ticket_medio", F.round(F.when(F.col("transacoes_pagas") > 0, F.col("valor_pago") / F.col("transacoes_pagas")), 2))
          .withColumn("participacao_pj_no_valor_pago",
                      F.round(F.when(F.col("valor_pago") > 0, F.col("vl_pagador_pj") / F.col("valor_pago")), 4)))
    w = Window.partitionBy("municipio_chave").orderBy("ano_mes")
    anterior = F.lag("valor_pago", 12).over(w)
    anterior_parcial = F.lag("mes_parcial", 12).over(w)
    mes_12_atras = F.lag("ano_mes", 12).over(w)
    comparavel = (~F.col("mes_parcial") & ~anterior_parcial
                  & (mes_12_atras == F.col("ano_mes") - 100))  # garante que não há mês faltando no meio
    df = df.withColumn("crescimento_anual",
                       F.when(comparavel, F.round((F.col("valor_pago") / anterior - 1).cast("double"), 4)))
    return df.select(
        "ano_mes", "municipio_chave", "municipio_ibge", "municipio", "uf", "regiao",
        "municipio_informado", "mes_parcial",
        "valor_pago", "transacoes_pagas", "ticket_medio", "participacao_pj_no_valor_pago",
        "crescimento_anual",
    )


def gold_uf_mes(silver_df: DataFrame) -> DataFrame:
    """Totais por UF e mês. A linha sem município fica num grupo próprio, não some."""
    return (silver_df
            .withColumn("uf", F.when(F.col("municipio_informado"), F.col("uf")).otherwise(F.lit("NAO INFORMADO")))
            .groupBy("ano_mes", "uf")
            .agg(F.sum(F.col("vl_pagador_pf") + F.col("vl_pagador_pj")).alias("valor_pago"),
                 F.sum(F.col("qt_pagador_pf") + F.col("qt_pagador_pj")).alias("transacoes_pagas"),
                 F.count("*").alias("municipios"),
                 F.max(F.col("mes_parcial").cast("int")).cast("boolean").alias("mes_parcial")))


# --- Dimensão de municípios conformada com o Projeto 13 (decisions/0005) ---

def dim_municipio(municipios: DataFrame, estados: DataFrame) -> DataFrame:
    """Dimensão de municípios a partir da tabela de referência do IBGE usada no Projeto 13.

    Uma linha por código do IBGE, mais o membro "não informado" com a mesma chave
    substituta da Silver (-1), para que a linha N/D do Pix também tenha par.
    `municipios` e `estados` são os CSVs de kelvins/municipios-brasileiros.
    """
    # O CSV de estados vem com BOM; o nome da primeira coluna chega como "﻿codigo_uf".
    estados = estados.toDF(*[c.lstrip("﻿") for c in estados.columns])
    municipios = municipios.toDF(*[c.lstrip("﻿") for c in municipios.columns])
    dim = (municipios.alias("m")
           .join(estados.alias("e"), F.col("m.codigo_uf").cast("int") == F.col("e.codigo_uf").cast("int"), "left")
           .select(
               F.col("m.codigo_ibge").cast("int").alias("municipio_chave"),
               F.col("m.codigo_ibge").cast("int").alias("municipio_ibge"),
               F.col("m.nome").alias("municipio_nome"),
               F.col("e.uf").alias("uf_sigla"),
               F.col("e.nome").alias("uf_nome"),
               F.col("e.regiao").alias("regiao_nome"),
               F.col("m.latitude").cast("double").alias("latitude"),
               F.col("m.longitude").cast("double").alias("longitude"),
               (F.col("m.capital").cast("int") == 1).alias("capital"),
           ))
    nao_informado = dim.sparkSession.createDataFrame(
        [(MUNICIPIO_NAO_INFORMADO, None, "Nao informado", None, "NAO INFORMADO", None, None, None, False)],
        dim.schema)
    return dim.unionByName(nao_informado)


def codigos_fora_da_dimensao(fato: DataFrame, dim: DataFrame) -> DataFrame:
    """Chaves do fato sem par na dimensão, com o nome que a fonte usa e os meses em que aparecem.

    Não vai para a quarentena: o dado do Pix é válido, quem está atrasada é a
    referência (decisions/0005). O notebook mostra a lista e falha se ela crescer.
    """
    return (fato.join(dim.select("municipio_chave"), "municipio_chave", "left_anti")
            .groupBy("municipio_chave", "municipio", "uf")
            .agg(F.min("ano_mes").alias("primeiro_mes"), F.countDistinct("ano_mes").alias("meses")))


def gold_municipio_mes_enriquecida(silver_df: DataFrame, dim: DataFrame) -> DataFrame:
    """Gold por município e mês com nome oficial, sigla da UF e coordenadas da dimensão."""
    atributos = dim.select("municipio_chave", "municipio_nome", "uf_sigla", "latitude", "longitude", "capital")
    return gold_municipio_mes(silver_df).join(atributos, "municipio_chave", "left")
