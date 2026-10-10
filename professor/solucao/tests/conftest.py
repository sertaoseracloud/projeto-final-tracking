import importlib
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

SOLUCAO = Path(__file__).resolve().parents[1]
if str(SOLUCAO) not in sys.path:
    sys.path.insert(0, str(SOLUCAO))


@pytest.fixture(scope="session")
def spark():
    sessao = (
        SparkSession.builder
        .master("local[2]")
        .appName("testes-solucao")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    sessao.sparkContext.setLogLevel("ERROR")
    yield sessao
    sessao.stop()


@pytest.fixture(scope="session")
def job():
    """Importa um job pelo nome do arquivo, ex.: job("02_silver_eventos")."""
    return importlib.import_module
