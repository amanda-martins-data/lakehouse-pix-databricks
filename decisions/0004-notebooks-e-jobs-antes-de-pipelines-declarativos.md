# 0004 · Notebooks e Jobs antes de pipelines declarativos

**Status:** aceita (24/11/2026)

## Contexto

O Databricks oferece dois jeitos de montar a Medallion: notebooks orquestrados
por um Job, com `MERGE` explícito, ou pipelines declarativos (Lakeflow
Declarative Pipelines, antes Delta Live Tables), em que se declaram as tabelas,
as expectativas de qualidade e o CDC, e a plataforma cuida da execução.

## Opções consideradas

**A. Notebooks + Job.** Cada passo é visível e testável fora do Databricks
(a lógica fica em `src/`, com pytest). O `MERGE` e a quarentena são escritos à
mão.

**B. Pipeline declarativo.** Menos código; expectativas e `APPLY CHANGES`
prontos. A lógica fica presa ao runtime do pipeline, mais difícil de testar
localmente. A Free Edition permite um pipeline ativo por tipo.

## Decisão

Opção A na primeira versão: deixa explícito o raciocínio de idempotência,
chave e quarentena, e roda nos testes locais e no CI.

## Consequências

- O mesmo `src/pix_lakehouse/transform.py` serve aos notebooks e aos testes.
- A versão declarativa entra como evolução, reaproveitando as regras de
  `validar` como expectativas, para comparar as duas abordagens com números.

**Revisar se:** a manutenção do `MERGE` manual ficar maior que o ganho de
testabilidade, ou na etapa de estudo para a certificação.
