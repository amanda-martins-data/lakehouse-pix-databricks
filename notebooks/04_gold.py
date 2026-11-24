# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Gold e conferências
# MAGIC Reconstrói as tabelas Gold a partir da Silver e confere que nada se perdeu no caminho.

# COMMAND ----------

import os
import sys

from pyspark.sql import functions as F

sys.path.append(os.path.abspath("../src"))
from pix_lakehouse import transform  # noqa: E402
from pix_lakehouse.schema import tabela  # noqa: E402

# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace")
catalogo = dbutils.widgets.get("catalogo")

silver = spark.table(tabela(catalogo, "silver", "transacoes_municipio"))
gold_municipio = tabela(catalogo, "gold", "pix_municipio_mes")
gold_uf = tabela(catalogo, "gold", "pix_uf_mes")

# COMMAND ----------

# Sem particionamento: são cerca de 400 mil linhas, e partição por mês criaria ~70 arquivos pequenos.
(transform.gold_municipio_mes(silver).write.mode("overwrite")
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

# Todo mês fechado deve ter 5.571 municípios e a linha N/D.
meses_incompletos = spark.sql(f"""
    SELECT ano_mes, count(*) AS linhas FROM {tabela(catalogo, 'silver', 'transacoes_municipio')}
    WHERE NOT mes_parcial GROUP BY ano_mes HAVING linhas < 5572
""").collect()
print("meses fechados com menos de 5.572 linhas:", [(r.ano_mes, r.linhas) for r in meses_incompletos])

display(spark.sql(f"""
    SELECT ano_mes, uf, round(valor_pago / 1e9, 2) AS valor_pago_bi, transacoes_pagas, mes_parcial
    FROM {gold_uf} WHERE ano_mes = (SELECT max(ano_mes) FROM {gold_uf} WHERE NOT mes_parcial)
    ORDER BY valor_pago DESC
"""))
