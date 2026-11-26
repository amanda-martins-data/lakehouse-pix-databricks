# Databricks notebook source
# MAGIC %md
# MAGIC # 03b · Dimensão de municípios
# MAGIC Monta `pix_silver.dim_municipio` a partir da mesma tabela de referência do IBGE usada no
# MAGIC [Projeto 13](https://github.com/amanda-martins-data/pipeline-multi-fonte-dados-publicos):
# MAGIC código de 7 dígitos, nome oficial, UF, região e coordenadas. Inclui o membro "não informado"
# MAGIC com a chave -1, a mesma da linha N/D na Silver (ver `decisions/0005`).
# MAGIC
# MAGIC Os CSVs ficam no volume, em `landing/referencia/`. O notebook tenta baixá-los; se o workspace
# MAGIC não alcançar o GitHub, usa os arquivos que já estiverem lá.

# COMMAND ----------

import os
import sys
import urllib.request
from pathlib import Path

sys.path.append(os.path.abspath("../src"))
from pix_lakehouse import transform  # noqa: E402
from pix_lakehouse.schema import REFERENCIA_ARQUIVOS, REFERENCIA_BASE, tabela  # noqa: E402

# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace")
catalogo = dbutils.widgets.get("catalogo")
pasta = Path(f"/Volumes/{catalogo}/pix_bronze/landing/referencia")
destino = tabela(catalogo, "silver", "dim_municipio")

# COMMAND ----------

pasta.mkdir(parents=True, exist_ok=True)
for nome in REFERENCIA_ARQUIVOS:
    try:
        urllib.request.urlretrieve(REFERENCIA_BASE + nome, pasta / nome)
        print(f"baixado: {nome}")
    except Exception as erro:  # sem saída para a internet: segue com o arquivo do volume
        assert (pasta / nome).exists(), f"{nome} não foi baixado ({erro}) e não está em {pasta}"
        print(f"usando o arquivo do volume: {nome} ({type(erro).__name__})")

# COMMAND ----------

ler = lambda nome: spark.read.option("header", True).option("encoding", "UTF-8").csv(str(pasta / nome))
dim = transform.dim_municipio(ler("municipios.csv"), ler("estados.csv"))

(dim.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(destino))
spark.sql(f"COMMENT ON TABLE {destino} IS 'Municipios do IBGE (mesma referencia do Projeto 13) e o membro nao informado (-1)'")

# Sem chave repetida, sem município sem UF, e pelo menos os 5.570 municípios oficiais.
d = spark.table(destino)
assert d.groupBy("municipio_chave").count().filter("count > 1").count() == 0, "chave repetida na dimensão"
assert d.filter("municipio_ibge IS NOT NULL AND uf_sigla IS NULL").count() == 0, "município sem UF"
assert d.filter("municipio_ibge IS NOT NULL").count() >= 5570, "referência com menos de 5.570 municípios"
display(d.groupBy("regiao_nome").count().orderBy("regiao_nome"))
