"""Testes da seleção do que levar ao Postgres, com Spark local (pulados sem Java/pyspark)."""

import shutil
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

pytest.importorskip("pyspark", reason="pyspark não instalado")
if shutil.which("java") is None:
    pytest.skip("Java não instalado", allow_module_level=True)

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "spark" / "jobs"))

from pyspark.sql import SparkSession  # noqa: E402

import silver_postgres  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    sessao = (
        SparkSession.builder.master("local[1]")
        .appName("testes_silver_postgres")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    sessao.sparkContext.setLogLevel("ERROR")
    yield sessao
    sessao.stop()


def test_so_as_particoes_reprocessadas_depois_da_ultima_carga(spark):
    antes = datetime(2026, 10, 1, tzinfo=UTC)
    depois = datetime(2026, 10, 3, tzinfo=UTC)
    silver = spark.createDataFrame(
        [
            {"ano": 2025, "mes": 1, "uf_sigla": "SP", "_processado_em": depois},
            {"ano": 2025, "mes": 1, "uf_sigla": "SP", "_processado_em": depois},
            {"ano": 2025, "mes": 1, "uf_sigla": "PR", "_processado_em": antes},
        ]
    )

    ultima_carga = datetime(2026, 10, 2, tzinfo=UTC)
    particoes = silver_postgres.particoes_reprocessadas(silver, ultima_carga)

    assert particoes == [(2025, 1, "SP")]
