import json
import os
import sys
import time
from pathlib import Path

import pytest

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "src"))

FIXTURES = RAIZ / "tests" / "fixtures"

# Datas sem fuso nos testes valem como UTC, igual à sessão do Spark.
os.environ["TZ"] = "UTC"
time.tzset()


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.master("local[1]").appName("testes-pix")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "1")
         .config("spark.ui.enabled", "false")
         .config("spark.driver.memory", "1g")
         # Os testes montam a Bronze com colunas literais (F.lit). Com isso o otimizador
         # deriva restrições demais ao unir duas Bronzes e estoura a memória num
         # computador pequeno. Com tabelas de verdade, como no Databricks, isso não acontece.
         .config("spark.sql.constraintPropagation.enabled", "false")
         .getOrCreate())
    yield s
    s.stop()


@pytest.fixture(scope="session")
def amostra():
    """23 linhas reais da API (Acre e a linha N/D), extraídas em 24/11/2026."""
    with (FIXTURES / "pix_amostra_real.jsonl").open(encoding="utf-8") as f:
        return [json.loads(l) for l in f]


@pytest.fixture(scope="session")
def dim(spark):
    """Dimensão montada com a referência real do IBGE: os 22 municípios do Acre.

    O CSV de estados mantém o BOM do arquivo original, de propósito.
    """
    from pix_lakehouse import transform
    ler = lambda nome: spark.read.option("header", True).option("encoding", "UTF-8").csv(str(FIXTURES / nome))
    return transform.dim_municipio(ler("municipios_ibge_acre.csv"), ler("estados_ibge.csv"))
