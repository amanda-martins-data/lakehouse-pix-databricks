# 0003 · Chave substituta para a linha sem município

**Status:** aceita (24/11/2026)

## Contexto

Todo mês, a fonte traz uma linha "N/D" com código do IBGE nulo, que soma
bilhões de reais (achado 3). A chave natural da Silver seria
`(ano_mes, municipio_ibge)`. Em SQL, `NULL = NULL` não é verdadeiro, então um
`MERGE` por essa chave nunca casa a linha N/D e a insere de novo a cada
reprocessamento (achado 4).

## Opções consideradas

**A. Descartar a linha N/D.** Os totais deixam de bater com os do Banco Central.

**B. `MERGE` com comparação nula-segura (`<=>`).** Resolve o `MERGE`, mas toda
consulta e toda junção posterior precisaria lembrar do mesmo cuidado.

**C. Chave substituta.** Criar `municipio_chave = coalesce(municipio_ibge, -1)`
e usar essa coluna em toda chave, mantendo `municipio_ibge` nulo e um indicador
`municipio_informado`.

## Decisão

Opção C. O valor -1 não colide com nenhum código do IBGE (sempre 7 dígitos).

## Consequências

- O `MERGE` é idempotente também para a linha N/D (teste
  `test_chave_nula_quebraria_o_merge`).
- Junções com tabelas do IBGE continuam pelo `municipio_ibge`; a linha N/D não
  casa, como deve ser.
- A Gold por UF mostra "NAO INFORMADO" como um grupo, e a soma de todas as UFs
  bate com a Silver (conferência no notebook 04).
