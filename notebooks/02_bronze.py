# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Bronze
# MAGIC Auto Loader lê os JSONL novos do volume e acrescenta à tabela Bronze, sem alterar o conteúdo.
# MAGIC O checkpoint garante que cada arquivo entra uma vez só, mesmo rodando de novo.

# COMMAND ----------

import os
import sys

from pyspark.sql import functions as F

sys.path.append(os.path.abspath("../src"))
from pix_lakehouse.schema import SCHEMA_ORIGEM, tabela  # noqa: E402
from pix_lakehouse.transform import extraido_em_do_caminho  # noqa: E402

# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace")
catalogo = dbutils.widgets.get("catalogo")

origem = f"/Volumes/{catalogo}/pix_bronze/landing/transacoes_municipio"
checkpoint = f"/Volumes/{catalogo}/pix_bronze/landing/_checkpoints/bronze_transacoes_municipio"
destino = tabela(catalogo, "bronze", "transacoes_municipio")

# COMMAND ----------

stream = (spark.readStream.format("cloudFiles")
          .option("cloudFiles.format", "json")
          .option("cloudFiles.schemaEvolutionMode", "rescue")   # campo novo na API vai para _rescued_data
          .schema(SCHEMA_ORIGEM)
          .load(origem)
          .withColumn("_arquivo", F.col("_metadata.file_path"))
          .withColumn("_extraido_em", extraido_em_do_caminho(F.col("_metadata.file_path")))
          .withColumn("_carregado_em", F.current_timestamp()))

consulta = (stream.writeStream
            .option("checkpointLocation", checkpoint)
            .trigger(availableNow=True)
            .toTable(destino))
consulta.awaitTermination()

# COMMAND ----------

display(spark.sql(f"""
    SELECT AnoMes, count(*) AS linhas, count(DISTINCT _arquivo) AS extracoes, max(_extraido_em) AS ultima_extracao
    FROM {destino} GROUP BY AnoMes ORDER BY AnoMes DESC LIMIT 6
"""))
