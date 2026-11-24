# Como rodar

## Etapa 1 · Databricks Free Edition

### 1. Conta e workspace

1. Crie a conta seguindo a [documentação da Free Edition](https://docs.databricks.com/aws/en/getting-started/free-edition)
   com um e-mail pessoal.
2. Anote o endereço do workspace (algo como `https://dbc-xxxx.cloud.databricks.com`)
   e coloque em `databricks.yml`, no target `free`.

### 2. CLI do Databricks no computador

```bash
# Instalação: https://docs.databricks.com/dev-tools/cli/install
databricks auth login --host https://SEU-WORKSPACE.cloud.databricks.com
databricks bundle validate -t free
```

O login abre o navegador (OAuth). Nenhum token fica salvo no repositório.

### 3. Publicar e rodar

```bash
databricks bundle deploy -t free
databricks bundle run pix_lakehouse -t free
```

Sem parâmetros, o Job extrai os três meses mais recentes. Para a carga
histórica (o Pix começou em novembro de 2020):

```bash
databricks bundle run pix_lakehouse -t free --params inicio=202011
```

### 4. Se o workspace não alcançar a API

A Free Edition só libera saída para domínios confiáveis. Se a tarefa
`extracao` falhar com erro de conexão:

```bash
pip install -r requirements-dev.txt
python scripts/baixar_pix.py --inicio 202011 --fim 202611
databricks fs cp -r data/raw/transacoes_municipio \
  dbfs:/Volumes/workspace/pix_bronze/landing/transacoes_municipio
databricks bundle deploy -t free --var pular_extracao=sim
databricks bundle run pix_lakehouse -t free
```

### 5. Conferir

O notebook `04_gold` falha se a soma da Gold não bater com a Silver.
Consultas úteis no SQL Editor:

```sql
-- Versões de cada mês na Bronze (quantas extrações)
SELECT AnoMes, count(DISTINCT _arquivo) AS extracoes
FROM workspace.pix_bronze.transacoes_municipio GROUP BY AnoMes ORDER BY AnoMes DESC;

-- O que caiu na quarentena e por quê
SELECT _motivo, count(*) FROM workspace.pix_silver.quarentena GROUP BY _motivo;

-- Viagem no tempo: a Silver antes do último MERGE
DESCRIBE HISTORY workspace.pix_silver.transacoes_municipio;
```

## Etapa 2 · Azure Databricks (fechamento)

A fazer: Terraform em `infra/azure` para resource group, workspace, ADLS Gen2,
Access Connector e external location no Unity Catalog. Rodar o bundle com
`-t azure`, documentar o resultado e destruir com `terraform destroy` no mesmo
dia (ver `decisions/0001`).

## Testes locais

```bash
pip install -r requirements-dev.txt   # precisa de Java 17 ou mais novo
pytest -q
```
