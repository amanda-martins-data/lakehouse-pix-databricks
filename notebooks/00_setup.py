# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup
# MAGIC Cria os schemas das três camadas e o volume de landing no Unity Catalog.
# MAGIC Idempotente: pode rodar de novo sem efeito colateral.

# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace")
catalogo = dbutils.widgets.get("catalogo")

# COMMAND ----------

for camada, comentario in [
    ("bronze", "Dado bruto da API do Banco Central, append-only, uma versao por extracao"),
    ("silver", "Dado tipado, validado e com a extracao mais recente de cada mes e municipio"),
    ("gold", "Metricas prontas para consumo: municipio x mes e UF x mes"),
]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalogo}.pix_{camada} COMMENT '{comentario}'")

spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {catalogo}.pix_bronze.landing
    COMMENT 'Arquivos JSONL extraidos da API, um por mes e por extracao'
""")

display(spark.sql(f"SHOW SCHEMAS IN {catalogo} LIKE 'pix_*'"))
