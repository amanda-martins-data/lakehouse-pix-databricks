# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Silver
# MAGIC Tipa, valida e aplica `MERGE` pela chave `(ano_mes, municipio_chave)`, ficando sempre com a
# MAGIC extração mais recente. Linhas inválidas vão para a quarentena com o motivo.
# MAGIC
# MAGIC A chave usa `municipio_chave` e não o código do IBGE: a linha "N/D" da fonte tem código nulo,
# MAGIC e `NULL = NULL` não casa no `MERGE` (ver `decisions/0003`).

# COMMAND ----------

import os
import sys

from delta.tables import DeltaTable
from pyspark.sql import functions as F

sys.path.append(os.path.abspath("../src"))
from pix_lakehouse import transform  # noqa: E402
from pix_lakehouse.schema import tabela  # noqa: E402

# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace")
catalogo = dbutils.widgets.get("catalogo")

bronze = spark.table(tabela(catalogo, "bronze", "transacoes_municipio"))
destino = tabela(catalogo, "silver", "transacoes_municipio")
quarentena_tbl = tabela(catalogo, "silver", "quarentena")

# COMMAND ----------

novos, quarentena = transform.silver(bronze)

if not spark.catalog.tableExists(destino):
    novos.limit(0).write.saveAsTable(destino)
    spark.sql(f"ALTER TABLE {destino} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

(DeltaTable.forName(spark, destino).alias("alvo")
 .merge(novos.alias("novo"),
        "alvo.ano_mes = novo.ano_mes AND alvo.municipio_chave = novo.municipio_chave")
 # Só atualiza se a extração for mais nova: reprocessar a Bronze inteira não regride nada.
 .whenMatchedUpdateAll(condition="novo._extraido_em > alvo._extraido_em")
 .whenNotMatchedInsertAll()
 .execute())

(quarentena.withColumn("_registrado_em", F.current_timestamp())
 .write.mode("append").option("mergeSchema", "true").saveAsTable(quarentena_tbl))

# COMMAND ----------

ultima = DeltaTable.forName(spark, destino).history(1).select("operationMetrics").first()[0]
print({k: ultima.get(k) for k in ("numTargetRowsInserted", "numTargetRowsUpdated", "numSourceRows")})

# Não pode haver chave repetida depois do MERGE.
repetidas = spark.sql(f"""
    SELECT ano_mes, municipio_chave, count(*) n FROM {destino}
    GROUP BY ALL HAVING n > 1
""").count()
assert repetidas == 0, f"{repetidas} chaves repetidas na Silver"
