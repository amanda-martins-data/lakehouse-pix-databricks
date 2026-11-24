# 0001 · Desenvolver na Databricks Free Edition e fechar no Azure Databricks

**Status:** aceita (24/11/2026)

## Contexto

O projeto precisa de Spark, Delta Lake, Unity Catalog e Jobs, que são o que
as vagas pedem e o que cai na certificação Databricks Data Engineer Associate.
Também precisa de infraestrutura como código, para conversar com o
[Projeto 04](https://github.com/amanda-martins-data/iac-pipeline-cloud-qualidade-ar)
e com o dia a dia em Azure. Não há orçamento recorrente para nuvem.

## Opções consideradas

**A. Só Free Edition.** Gratuita e sem prazo. Só computação serverless, um
workspace, um SQL warehouse 2X-Small, uso diário limitado, sem console de conta
nem API de conta (então sem Terraform para a plataforma), uso não comercial e
acesso de saída só a domínios confiáveis.

**B. Só Azure Databricks.** Plataforma completa, integrada a ADLS Gen2, Key Vault
e Entra ID, provisionável com Terraform. A avaliação dá até US$ 400 em créditos
por 14 dias; depois disso, cobra na assinatura. Desenvolver por semanas ali
consome os créditos com tempo ocioso.

**C. Híbrido.** Desenvolver e testar na Free Edition; no fim, provisionar o
Azure Databricks com Terraform, rodar o mesmo bundle, documentar o resultado e
destruir a infraestrutura.

## Decisão

Opção C. O código é o mesmo nas duas etapas: o Databricks Asset Bundle tem um
target `free` e um `azure`, e o catálogo é um parâmetro.

## Consequências

- Custo zero no desenvolvimento; os créditos do Azure ficam para uma execução
  curta e planejada.
- A etapa Azure tem de ser feita de uma vez e destruída no mesmo dia, como no
  [Projeto 14](https://github.com/amanda-martins-data/snowflake-medallion-qualidade-ar).
- Se a Free Edition não alcançar a API do Banco Central, a extração roda no
  computador (`scripts/baixar_pix.py`) e os arquivos vão ao volume pelo CLI.
  O notebook de extração tem o parâmetro `pular` para esse caso.

**Revisar se:** a Free Edition mudar os limites, ou o projeto passar a precisar
de recursos que ela não oferece (clusters clássicos, rede privada).
