# Achados da fonte

Fonte: API OData do Banco Central, recurso `TransacoesPixPorMunicipio`
([dados abertos do Pix](https://dadosabertos.bcb.gov.br/dataset/pix)).
Medições feitas em 24/11/2026, direto na API.

## 1. `DataBase` quer dizer "a partir de", não "o mês de"

A URL documentada recebe um parâmetro `DataBase` no formato `AAAAMM`.
Parece pedir um mês, mas devolve todos os meses daquele em diante.

| Pedido | Linhas | Meses devolvidos |
|---|---|---|
| `DataBase=202510` | 78.008 | 14 (202510 a 202611) |
| `DataBase=202001` | 200.000 (o `$top` pedido) | 73 (202011 a 202611) |

Uma extração que confiasse no nome do parâmetro gravaria 14 meses num arquivo
chamado "outubro de 2025".

**Tratamento:** cada requisição fixa o mês também com `$filter=AnoMes eq AAAAMM`,
e o extrator recusa a resposta se vier qualquer linha de outro mês
(`extract.extrair_mes`, testado em `test_recusa_resposta_com_outro_mes`).

## 2. O mês corrente é publicado antes de fechar

Em 24/11/2026 a API já tinha novembro de 2026. Para São Paulo (capital), o valor
pago por pessoas físicas no mês foi:

| Mês | Valor pago por PF (R$ bi) |
|---|---|
| 202609 | 89,3 |
| 202610 | 94,2 |
| 202611 | 70,8 (dia 24) |

Novembro aparece com cerca de 75% de outubro porque o mês ainda não acabou.
Comparar esse número com o mês anterior daria uma "queda" de 25% que não existe.

**Tratamento:** cada arquivo leva a data da extração no nome; a Silver marca
`mes_parcial` quando o mês ainda não tinha terminado na extração, e o
crescimento anual da Gold só compara meses completos. O job reextrai os três
meses mais recentes a cada execução, e o `MERGE` fica com a versão mais nova.

## 3. Uma linha sem município carrega bilhões

Todo mês tem 5.572 linhas: 5.571 municípios e uma linha com `Municipio = "N/D"`,
código do IBGE nulo e UF "NAO INFORMADO". Em novembro de 2026 (parcial), essa
linha somava R$ 7,9 bilhões pagos.

Descartar a linha por ter código nulo faria os totais por UF e Brasil não
baterem com os do Banco Central. Mantê-la com código nulo quebra o `MERGE`
(ver o achado 4).

**Tratamento:** a linha fica, com `municipio_chave = -1` e
`municipio_informado = false`, e entra na Gold por UF num grupo próprio.

## 4. Chave nula duplica a linha a cada reprocessamento

Em SQL, `NULL = NULL` não é verdadeiro. Um `MERGE ... ON alvo.municipio_ibge = novo.municipio_ibge`
nunca encontraria a linha N/D já gravada e a inseriria de novo em toda execução.
Como o job reprocessa três meses por vez, a linha se multiplicaria em silêncio.

**Tratamento:** o `MERGE` usa `municipio_chave` (código do IBGE ou -1).
O teste `test_chave_nula_quebraria_o_merge` mostra a diferença com a amostra real:
pela chave da fonte, 22 de 23 linhas casam; pela chave tratada, 23 de 23.

## 5. São 5.571 municípios, não 5.570

O número oficial de municípios é 5.570, mas a fonte traz 5.571 códigos.
A tabela do IBGE usada no
[Projeto 13](https://github.com/amanda-martins-data/pipeline-multi-fonte-dados-publicos)
também tem 5.571 linhas, porque o distrito estadual de Fernando de Noronha tem
código próprio; é a explicação mais provável aqui também, a confirmar na
junção com o IBGE.

## 6. O primeiro mês é parcial por outro motivo

O Pix começou a funcionar em 16/11/2020. Novembro de 2020 tem metade do mês
e 5.569 linhas (provavelmente municípios sem nenhuma transação nas duas
primeiras semanas), e o
crescimento contra ele seria enganoso. A Silver o marca como parcial.

## 7. A API não pagina

Qualquer requisição com `$skip`, mesmo `$skip=0`, volta com erro 500 e a
mensagem "Erro desconhecido". Não há como buscar uma resposta em partes.

**Tratamento:** um mês por requisição (no máximo 5.572 linhas) com `$top=10000`.
Se a resposta vier com 10.000 linhas, o extrator falha em vez de gravar um mês
possivelmente cortado (`test_falha_se_a_resposta_vier_cheia`).
