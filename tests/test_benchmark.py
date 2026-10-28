"""Testes do benchmark: dados sintéticos e equivalência pandas × Spark.

Pulados sem pyspark e Java, como os outros testes que usam o Spark.
"""

import shutil
import sys
from pathlib import Path

import pytest

pytest.importorskip("pyspark", reason="pyspark não instalado")
if shutil.which("java") is None:
    pytest.skip("Java não instalado", allow_module_level=True)

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "spark" / "jobs"))

from pyspark.sql import SparkSession  # noqa: E402

import benchmark  # noqa: E402
import silver_clima  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    sessao = (
        SparkSession.builder.master("local[1]")
        .appName("testes_benchmark")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    sessao.sparkContext.setLogLevel("ERROR")
    yield sessao
    sessao.stop()


def test_bronze_sintetico_tem_o_tamanho_pedido_e_as_repeticoes():
    bronze, municipios = benchmark.gerar_bronze(10_000)

    repetidas = int(10_000 * benchmark.PROPORCAO_DUPLICADAS)
    assert len(bronze) == 10_000 + repetidas
    assert bronze.duplicated(subset=silver_clima.CHAVE).sum() == repetidas
    assert len(municipios) == benchmark.N_MUNICIPIOS


def test_pandas_deduplica_ficando_com_a_extracao_mais_nova():
    bronze, municipios = benchmark.gerar_bronze(10_000)

    silver = benchmark.transformar_pandas(bronze, municipios)

    assert len(silver) == 10_000
    assert list(silver.columns) == silver_clima.COLUNAS_SILVER


def test_pandas_e_spark_chegam_ao_mesmo_resultado(spark):
    bronze, municipios = benchmark.gerar_bronze(10_000)

    silver_pandas = benchmark.transformar_pandas(bronze, municipios)
    silver_spark = silver_clima.transformar(
        spark.createDataFrame(bronze), spark.createDataFrame(municipios)
    )

    benchmark.conferir_equivalencia(silver_pandas, silver_spark)  # levanta erro se divergir


def test_equivalencia_acusa_divergencia(spark):
    bronze, municipios = benchmark.gerar_bronze(10_000)
    silver_spark = silver_clima.transformar(
        spark.createDataFrame(bronze), spark.createDataFrame(municipios)
    )
    silver_pandas = benchmark.transformar_pandas(bronze, municipios).iloc[:-1]  # uma a menos

    with pytest.raises(AssertionError, match="divergiram"):
        benchmark.conferir_equivalencia(silver_pandas, silver_spark)
