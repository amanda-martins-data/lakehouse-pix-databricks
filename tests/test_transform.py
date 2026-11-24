from datetime import datetime

from pyspark.sql import Row
from pyspark.sql import functions as F

from pix_lakehouse import transform as t
from pix_lakehouse.schema import MUNICIPIO_NAO_INFORMADO, SCHEMA_ORIGEM

EXTRAIDO = datetime(2026, 11, 24, 23, 10, 5)
ARQUIVO = "/Volumes/x/ano_mes=202611/pix_municipio_202611_20261124T231005Z.jsonl"


def bronze(spark, linhas, extraido=EXTRAIDO, arquivo=ARQUIVO):
    df = spark.createDataFrame(linhas, SCHEMA_ORIGEM)
    return df.withColumn("_arquivo", F.lit(arquivo)).withColumn("_extraido_em", F.lit(extraido))


def linha(ano_mes, municipio, valor=100.0, qt=10, **extra):
    base = {c: 0 for c in ["QT_PagadorPJ", "QT_RecebedorPF", "QT_RecebedorPJ", "QT_PES_PagadorPF",
                           "QT_PES_PagadorPJ", "QT_PES_RecebedorPF", "QT_PES_RecebedorPJ"]}
    base.update({c: 0.0 for c in ["VL_PagadorPJ", "VL_RecebedorPF", "VL_RecebedorPJ"]})
    base.update(AnoMes=ano_mes, Municipio_Ibge=municipio, Municipio=f"M{municipio}",
                Estado_Ibge=12 if municipio else None, Estado="ACRE" if municipio else "NAO INFORMADO",
                Sigla_Regiao="NO", Regiao="NORTE", VL_PagadorPF=valor, QT_PagadorPF=qt)
    base.update(extra)
    return base


def test_extraido_em_vem_do_nome_do_arquivo(spark):
    df = spark.createDataFrame([(ARQUIVO,)], ["p"]).select(t.extraido_em_do_caminho(F.col("p")).alias("e"))
    assert df.first().e == EXTRAIDO


def test_linha_sem_municipio_ganha_chave_sentinela(spark, amostra):
    s = t.padronizar(bronze(spark, amostra))
    nd = s.filter(~F.col("municipio_informado")).collect()
    assert len(nd) == 1
    assert nd[0].municipio_chave == MUNICIPIO_NAO_INFORMADO
    assert nd[0].municipio_ibge is None


def test_mes_da_extracao_e_parcial_e_os_anteriores_nao(spark, amostra):
    s = t.padronizar(bronze(spark, amostra))
    por_mes = {r.ano_mes: r.p for r in s.groupBy("ano_mes").agg(F.max("mes_parcial").alias("p")).collect()}
    assert por_mes == {202609: False, 202610: False, 202611: True}


def test_novembro_de_2020_e_parcial_porque_o_pix_comecou_no_dia_16(spark):
    s = t.padronizar(bronze(spark, [linha(202011, 1200013)]))
    assert s.first().mes_parcial is True


def test_amostra_real_passa_inteira_na_validacao(spark, amostra):
    validos, quarentena = t.validar(t.padronizar(bronze(spark, amostra)))
    assert validos.count() == len(amostra)
    assert quarentena.count() == 0


def test_quarentena_explica_o_motivo(spark):
    ruins = [linha(202610, 1200013, valor=-5.0),
             linha(202610, 12345),
             linha(201901, 1200013)]
    validos, quarentena = t.validar(t.padronizar(bronze(spark, ruins)))
    motivos = sorted(r._motivo for r in quarentena.collect())
    assert validos.count() == 0
    assert motivos == ["ano_mes fora do intervalo", "codigo IBGE sem 7 digitos", "valor ou quantidade negativa"]


def test_vale_a_extracao_mais_recente(spark):
    antiga = bronze(spark, [linha(202611, 1200013, valor=70.0)],
                    extraido=datetime(2026, 11, 24), arquivo="a_20261124T000000Z.jsonl")
    nova = bronze(spark, [linha(202611, 1200013, valor=100.0)],
                  extraido=datetime(2026, 12, 2), arquivo="a_20261202T000000Z.jsonl")
    s, _ = t.silver(antiga.unionByName(nova))
    r = s.collect()
    assert len(r) == 1 and float(r[0].vl_pagador_pf) == 100.0
    # Extraído em dezembro, novembro passa a ser mês fechado.
    assert r[0].mes_parcial is False


def test_chave_nula_quebraria_o_merge(spark, amostra):
    """Por que existe municipio_chave: em SQL, NULL = NULL não é verdadeiro.

    Um MERGE pela chave da fonte nunca casaria a linha N/D consigo mesma, e cada
    reprocessamento a inseriria de novo. Aqui a junção simula o ON do MERGE.
    """
    s = t.padronizar(bronze(spark, amostra)).alias("s")
    alvo = t.padronizar(bronze(spark, amostra)).alias("a")
    pela_fonte = s.join(alvo, ["ano_mes", "municipio_ibge"]).count()
    pela_chave = s.join(alvo, ["ano_mes", "municipio_chave"]).count()
    assert pela_chave == len(amostra)
    assert pela_fonte == len(amostra) - 1   # a linha N/D não casa


def test_crescimento_anual_so_entre_meses_completos(spark):
    linhas = [linha(m, 1200013, valor=100.0) for m in
              [202510, 202511, 202512, 202601, 202602, 202603, 202604, 202605, 202606, 202607, 202608, 202609]]
    linhas.append(linha(202610, 1200013, valor=110.0))
    linhas.append(linha(202611, 1200013, valor=80.0))   # parcial
    g = {r.ano_mes: r.crescimento_anual for r in t.gold_municipio_mes(t.silver(bronze(spark, linhas))[0]).collect()}
    assert g[202610] == 0.1
    assert g[202611] is None          # mês parcial não entra na comparação
    assert g[202609] is None          # não há 12 meses antes


def test_crescimento_anual_nao_compara_se_falta_um_mes_no_meio(spark):
    meses = [202509, 202511, 202512, 202601, 202602, 202603, 202604, 202605, 202606, 202607, 202608, 202609, 202610]
    linhas = [linha(m, 1200013, valor=100.0) for m in meses]
    g = {r.ano_mes: r.crescimento_anual for r in t.gold_municipio_mes(t.silver(bronze(spark, linhas))[0]).collect()}
    # O 12o mês anterior na fila é 202509, não 202510: a comparação seria falsa.
    assert g[202610] is None


def test_ticket_medio_e_participacao_pj(spark):
    r = t.gold_municipio_mes(t.silver(bronze(spark, [linha(202610, 1200013, valor=300.0, qt=3,
                                                           VL_PagadorPJ=100.0, QT_PagadorPJ=1)]))[0]).first()
    assert float(r.valor_pago) == 400.0
    assert float(r.ticket_medio) == 100.0
    assert float(r.participacao_pj_no_valor_pago) == 0.25


def test_gold_uf_mantem_a_linha_sem_municipio(spark, amostra):
    g = t.gold_uf_mes(t.silver(bronze(spark, amostra))[0])
    ufs = {(r.ano_mes, r.uf) for r in g.collect()}
    assert (202611, "NAO INFORMADO") in ufs
    total_silver = t.silver(bronze(spark, amostra))[0].agg(
        F.sum(F.col("vl_pagador_pf") + F.col("vl_pagador_pj"))).first()[0]
    total_gold = g.agg(F.sum("valor_pago")).first()[0]
    assert total_gold == total_silver   # nada se perde na agregação
