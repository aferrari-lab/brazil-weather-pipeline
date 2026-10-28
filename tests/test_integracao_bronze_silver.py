"""Teste de integração: o bronze gravado pela ingestão é lido e tratado pela silver.

Junta as duas pontas que, sozinhas, são testadas em arquivos separados: a ingestão
(pandas/pyarrow) grava o bronze numa pasta local, e o job Spark da silver lê esse
mesmo bronze. Pulado sem pyspark e Java.
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

import silver_clima  # noqa: E402
from ingestion import clima  # noqa: E402
from ingestion.lake import ConfigLake, gravar_parquet  # noqa: E402
from ingestion.municipios import CAMINHO_MUNICIPIOS  # noqa: E402
from tests.test_clima import FIM, INICIO, MUNICIPIOS, _SessaoFalsa  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    sessao = (
        SparkSession.builder.master("local[1]")
        .appName("teste_integracao")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    sessao.sparkContext.setLogLevel("ERROR")
    yield sessao
    sessao.stop()


def test_bronze_da_ingestao_vira_silver_sem_duplicatas(spark, tmp_path):
    lake = ConfigLake(raiz=str(tmp_path / "lake"))
    sp = MUNICIPIOS[MUNICIPIOS["uf_sigla"] == "SP"]
    # O cadastro real tem a região; os municípios de exemplo da ingestão, não.
    gravar_parquet(MUNICIPIOS.assign(regiao="Sudeste"), CAMINHO_MUNICIPIOS, lake)

    # Carga normal e depois um reprocessamento: o bronze fica com duas versões.
    clima.carregar_clima_uf(sp, INICIO, FIM, _SessaoFalsa(), lake, pausa_segundos=0)
    clima.carregar_clima_uf(
        sp, INICIO, FIM, _SessaoFalsa(), lake, pausa_segundos=0, reprocessar=True
    )

    raiz = tmp_path / "lake"
    resumo = silver_clima.processar(
        spark,
        str(raiz / clima.CAMINHO_CLIMA),
        str(raiz / CAMINHO_MUNICIPIOS),
        str(raiz / "silver"),
    )

    assert resumo["linhas"] == 2 * 4  # 2 cidades × 4 dias, uma linha por dia
    silver = spark.read.parquet(str(raiz / "silver"))
    assert silver.filter("municipio IS NULL").count() == 0
    assert silver.filter("_gravado_em IS NULL").count() == 0
