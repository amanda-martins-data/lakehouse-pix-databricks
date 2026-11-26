# Achados da fonte

Fonte: API OData do Banco Central, recurso `TransacoesPixPorMunicipio`
([dados abertos do Pix](https://dadosabertos.bcb.gov.br/dataset/pix)).
Achados 1 a 4 e 7 medidos em 24/11/2026, direto na API. Achados 5, 6 e 8
corrigidos ou descobertos na primeira carga completa, em 26/11/2026
(73 meses, 406.591 linhas).

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

Todo mês tem exatamente uma linha com `Municipio = "N/D"`, código do IBGE nulo
e UF "NAO INFORMADO", além das linhas dos municípios (ver o achado 5). Em novembro de 2026 (parcial), essa
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

## 5. A lista de municípios muda com o tempo

A primeira versão deste documento dizia que todo mês tinha 5.571 municípios.
A carga completa mostrou outra coisa:

| Meses | Municípios | Linhas (com a N/D) |
|---|---|---|
| 202011 a 202503 (53 meses) | 5.568 | 5.569 |
| 202504 a 202509 (6 meses) | 5.570 | 5.571 |
| 202510 a 202611 (14 meses) | 5.571 | 5.572 |

Três códigos só aparecem a partir de certo mês:

| Código IBGE | Município | Primeiro mês |
|---|---|---|
| 2405306 | Januário Cicco (RN) | 202504 |
| 2401305 | Campo Grande (RN) | 202504 |
| 5101837 | Boa Esperança do Norte (MT) | 202510 |

Boa Esperança do Norte é um município recente. Os dois do Rio Grande do Norte
são municípios antigos; para onde iam as transações deles antes de abril de
2025 a fonte não diz, e a hipótese mais provável é a linha N/D. Fernando de
Noronha (2605459) aparece em todos os meses.

**Tratamento:** a validação da Gold não exige um número fixo de linhas. Ela
falha se um mês não tiver exatamente uma linha N/D ou se tiver menos
municípios que o mês anterior, o que indicaria dado faltando. A comparação
com a tabela de municípios do IBGE fica para a dimensão conformada com o
[Projeto 13](https://github.com/amanda-martins-data/pipeline-multi-fonte-dados-publicos).

## 6. O primeiro mês é parcial por outro motivo

O Pix começou a funcionar em 16/11/2020. Novembro de 2020 tem só metade do
mês, e o crescimento contra ele seria enganoso. A Silver o marca como parcial.
(Ele tem 5.569 linhas como os meses seguintes; a primeira versão deste
documento atribuía o número a municípios sem transação, o que a carga
completa desmentiu.)

## 7. A API não pagina

Qualquer requisição com `$skip`, mesmo `$skip=0`, volta com erro 500 e a
mensagem "Erro desconhecido". Não há como buscar uma resposta em partes.

**Tratamento:** um mês por requisição (no máximo 5.572 linhas) com `$top=10000`.
Se a resposta vier com 10.000 linhas, o extrator falha em vez de gravar um mês
possivelmente cortado (`test_falha_se_a_resposta_vier_cheia`).

## 8. A Free Edition não alcança a API

Na Databricks Free Edition, a extração falhou com `URLError`: o workspace só
libera saída para alguns domínios, e `olinda.bcb.gov.br` não está entre eles
(testado em 26/11/2026).

**Tratamento:** no target `free` do bundle, `pular_extracao` vale `sim`. Os
arquivos chegam ao volume por fora, no mesmo formato e com o mesmo nome que o
extrator gravaria, e o Job começa na Bronze. No Azure Databricks a extração
roda dentro do Job.
