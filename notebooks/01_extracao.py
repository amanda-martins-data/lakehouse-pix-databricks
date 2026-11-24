# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Extração
# MAGIC Baixa da API do Banco Central os meses pedidos e grava um JSONL por mês no volume de landing.
# MAGIC
# MAGIC Por padrão reextrai os **três meses mais recentes**: o mês corrente é publicado antes de fechar
# MAGIC e os anteriores podem ser revisados (ver `decisions/0002`).
# MAGIC
# MAGIC Se o workspace não tiver acesso de saída ao `olinda.bcb.gov.br` (a Free Edition só libera
# MAGIC domínios confiáveis), rode `scripts/baixar_pix.py` no computador e envie os arquivos ao volume.
# MAGIC Nesse caso, desative esta tarefa com o parâmetro `pular=sim`.

# COMMAND ----------

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.append(os.path.abspath("../src"))
from pix_lakehouse import extract  # noqa: E402

# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace")
dbutils.widgets.text("inicio", "")      # AAAAMM; vazio = dois meses antes do atual
dbutils.widgets.text("fim", "")         # AAAAMM; vazio = mês atual
dbutils.widgets.dropdown("pular", "nao", ["nao", "sim"])

catalogo = dbutils.widgets.get("catalogo")
mes_atual = int(datetime.now(timezone.utc).strftime("%Y%m"))
fim = int(dbutils.widgets.get("fim") or mes_atual)
inicio = int(dbutils.widgets.get("inicio") or extract.mes_anterior(fim, 2))
destino = Path(f"/Volumes/{catalogo}/pix_bronze/landing/transacoes_municipio")

# COMMAND ----------

if dbutils.widgets.get("pular") == "sim":
    dbutils.notebook.exit("extracao pulada: arquivos enviados manualmente ao volume")

feitas = extract.extrair_periodo(inicio, fim, destino)
for f in feitas:
    print(f"{f.ano_mes}: {f.linhas} linhas -> {f.arquivo}")

# Um mês completo tem 5.572 linhas (5.571 municípios e a linha N/D). Bem menos indica falha da fonte.
suspeitos = [f for f in feitas if f.linhas < 5000]
assert not suspeitos, f"meses com poucas linhas: {[(f.ano_mes, f.linhas) for f in suspeitos]}"
