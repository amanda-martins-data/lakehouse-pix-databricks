# 0002 · Reprocessar os três meses mais recentes a cada execução

**Status:** aceita (24/11/2026)

## Contexto

A API publica o mês corrente antes de ele fechar (achado 2 em
`docs/achados-da-fonte.md`). Um mês extraído no dia 24 fica com cerca de 75%
do valor final. Não há documentação sobre revisões de meses já fechados.

## Opções consideradas

**A. Extrair só meses fechados.** Simples, mas atrasa o dado em até um mês
e não resolve uma eventual revisão posterior.

**B. Extrair o mês corrente e sobrescrever a tabela.** Perde o histórico de
versões e mistura parcial com fechado sem marcar a diferença.

**C. Janela móvel com versões.** Reextrair os três meses mais recentes a cada
execução, gravar cada extração como arquivo novo na Bronze (com a data no nome),
e na Silver fazer `MERGE` que só atualiza quando a extração é mais nova.

## Decisão

Opção C, com janela de três meses e a marcação `mes_parcial` na Silver.

## Consequências

- A Bronze guarda todas as versões, então dá para medir se o Banco Central
  revisa meses fechados: basta comparar extrações do mesmo mês.
- O crescimento anual na Gold só compara meses completos.
- Reprocessar a Bronze inteira é seguro: o `MERGE` não regride para uma
  extração mais antiga.

**Revisar se:** a comparação entre extrações mostrar revisões além de três
meses (aumentar a janela) ou nenhuma revisão em meses fechados (reduzir para dois).
