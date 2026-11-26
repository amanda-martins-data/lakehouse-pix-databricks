# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Gold e conferências
# MAGIC Reconstrói as tabelas Gold a partir da Silver e confere que nada se perdeu no caminho.
# MAGIC A Gold por município recebe nome oficial, sigla da UF e coordenadas da `dim_municipio`.

# COMMAND ----------

import os
import sys

from pyspark.sql import functions as F

sys.path.append(os.path.abspath("../src"))
from pix_lakehouse import transform  # noqa: E402
from pix_lakehouse.schema import CODIGOS_FORA_DA_REFERENCIA, tabela  # noqa: E402

# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace")
catalogo = dbutils.widgets.get("catalogo")

silver = spark.table(tabela(catalogo, "silver", "transacoes_municipio"))
dim = spark.table(tabela(catalogo, "silver", "dim_municipio"))
gold_municipio = tabela(catalogo, "gold", "pix_municipio_mes")
gold_uf = tabela(catalogo, "gold", "pix_uf_mes")

# COMMAND ----------

# Sem particionamento: são cerca de 400 mil linhas, e partição por mês criaria ~70 arquivos pequenos.
(transform.gold_municipio_mes_enriquecida(silver, dim).write.mode("overwrite")
 .option("overwriteSchema", "true").saveAsTable(gold_municipio))
transform.gold_uf_mes(silver).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold_uf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conferências

# COMMAND ----------

def total(df, col):
    return df.agg(F.sum(col)).first()[0]

valor_silver = total(silver, F.col("vl_pagador_pf") + F.col("vl_pagador_pj"))
assert total(spark.table(gold_municipio), "valor_pago") == valor_silver, "Gold por municipio diverge da Silver"
assert total(spark.table(gold_uf), "valor_pago") == valor_silver, "Gold por UF diverge da Silver"

# O número de municípios muda com o tempo (5.568 até 202503, 5.570 até 202509, 5.571 depois;
# ver docs/achados-da-fonte.md). O que não pode acontecer: mês sem exatamente uma linha N/D,
# ou um mês com menos municípios que o anterior, o que indicaria dado faltando.
problemas = spark.sql(f"""
    WITH m AS (
        SELECT ano_mes,
               count_if(municipio_chave = -1) AS linhas_nd,
               count_if(municipio_chave <> -1) AS municipios
        FROM {tabela(catalogo, 'silver', 'transacoes_municipio')} GROUP BY ano_mes)
    SELECT * FROM (
        SELECT ano_mes, linhas_nd, municipios,
               lag(municipios) OVER (ORDER BY ano_mes) AS municipios_mes_anterior
        FROM m)
    WHERE linhas_nd <> 1 OR municipios < municipios_mes_anterior
""").collect()
assert not problemas, f"meses com N/D ausente ou repetido, ou com municipios a menos: {problemas}"

# Conformidade com a dimensão (decisions/0005): código do Pix sem par na referência do IBGE.
# Não vai para a quarentena; a lista é conhecida e o notebook falha se aparecer um código novo.
fora = transform.codigos_fora_da_dimensao(silver, dim).collect()
print("codigos do Pix fora da referencia do IBGE:", [(r.municipio_chave, r.municipio, r.uf, r.primeiro_mes) for r in fora])
novos_fora = {r.municipio_chave for r in fora} - CODIGOS_FORA_DA_REFERENCIA
assert not novos_fora, f"codigos novos fora da referencia do IBGE: {sorted(novos_fora)}"

display(spark.sql(f"""
    SELECT ano_mes, uf, round(valor_pago / 1e9, 2) AS valor_pago_bi, transacoes_pagas, mes_parcial
    FROM {gold_uf} WHERE ano_mes = (SELECT max(ano_mes) FROM {gold_uf} WHERE NOT mes_parcial)
    ORDER BY valor_pago DESC
"""))
