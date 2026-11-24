"""Contrato da fonte e nomes das tabelas.

Os campos de origem são os da API OData do Banco Central
(recurso TransacoesPixPorMunicipio). Na Silver eles ganham nomes em
snake_case e tipos explícitos.
"""
from pyspark.sql import types as T

# Campos exatamente como chegam da API, na ordem em que a API devolve.
CAMPOS_ORIGEM = [
    "AnoMes", "Municipio_Ibge", "Municipio", "Estado_Ibge", "Estado",
    "Sigla_Regiao", "Regiao",
    "VL_PagadorPF", "QT_PagadorPF", "VL_PagadorPJ", "QT_PagadorPJ",
    "VL_RecebedorPF", "QT_RecebedorPF", "VL_RecebedorPJ", "QT_RecebedorPJ",
    "QT_PES_PagadorPF", "QT_PES_PagadorPJ", "QT_PES_RecebedorPF", "QT_PES_RecebedorPJ",
]

SCHEMA_ORIGEM = T.StructType([
    T.StructField("AnoMes", T.LongType()),
    T.StructField("Municipio_Ibge", T.LongType()),
    T.StructField("Municipio", T.StringType()),
    T.StructField("Estado_Ibge", T.LongType()),
    T.StructField("Estado", T.StringType()),
    T.StructField("Sigla_Regiao", T.StringType()),
    T.StructField("Regiao", T.StringType()),
    T.StructField("VL_PagadorPF", T.DoubleType()),
    T.StructField("QT_PagadorPF", T.LongType()),
    T.StructField("VL_PagadorPJ", T.DoubleType()),
    T.StructField("QT_PagadorPJ", T.LongType()),
    T.StructField("VL_RecebedorPF", T.DoubleType()),
    T.StructField("QT_RecebedorPF", T.LongType()),
    T.StructField("VL_RecebedorPJ", T.DoubleType()),
    T.StructField("QT_RecebedorPJ", T.LongType()),
    T.StructField("QT_PES_PagadorPF", T.LongType()),
    T.StructField("QT_PES_PagadorPJ", T.LongType()),
    T.StructField("QT_PES_RecebedorPF", T.LongType()),
    T.StructField("QT_PES_RecebedorPJ", T.LongType()),
])

# Renomeação para a Silver.
RENOMEAR = {
    "AnoMes": "ano_mes",
    "Municipio_Ibge": "municipio_ibge",
    "Municipio": "municipio",
    "Estado_Ibge": "uf_ibge",
    "Estado": "uf",
    "Sigla_Regiao": "regiao_sigla",
    "Regiao": "regiao",
    "VL_PagadorPF": "vl_pagador_pf",
    "QT_PagadorPF": "qt_pagador_pf",
    "VL_PagadorPJ": "vl_pagador_pj",
    "QT_PagadorPJ": "qt_pagador_pj",
    "VL_RecebedorPF": "vl_recebedor_pf",
    "QT_RecebedorPF": "qt_recebedor_pf",
    "VL_RecebedorPJ": "vl_recebedor_pj",
    "QT_RecebedorPJ": "qt_recebedor_pj",
    "QT_PES_PagadorPF": "pessoas_pagadoras_pf",
    "QT_PES_PagadorPJ": "pessoas_pagadoras_pj",
    "QT_PES_RecebedorPF": "pessoas_recebedoras_pf",
    "QT_PES_RecebedorPJ": "pessoas_recebedoras_pj",
}

CAMPOS_VALOR = ["vl_pagador_pf", "vl_pagador_pj", "vl_recebedor_pf", "vl_recebedor_pj"]
CAMPOS_QUANTIDADE = [
    "qt_pagador_pf", "qt_pagador_pj", "qt_recebedor_pf", "qt_recebedor_pj",
    "pessoas_pagadoras_pf", "pessoas_pagadoras_pj", "pessoas_recebedoras_pf", "pessoas_recebedoras_pj",
]

# A fonte publica uma linha sem município ("N/D", código nulo). Ela não pode
# entrar na chave de MERGE como nulo (ver decisions/0003), então ganha um
# código sentinela fora do intervalo dos códigos do IBGE.
MUNICIPIO_NAO_INFORMADO = -1

# Chave natural de um registro na Silver.
CHAVE = ["ano_mes", "municipio_chave"]

# Mês de lançamento do Pix: 16/11/2020. Novembro de 2020 é um mês parcial.
PRIMEIRO_MES = 202011


def tabela(catalogo: str, camada: str, nome: str) -> str:
    """Nome completo de uma tabela no Unity Catalog, por exemplo workspace.pix_silver.transacoes."""
    return f"{catalogo}.pix_{camada}.{nome}"
