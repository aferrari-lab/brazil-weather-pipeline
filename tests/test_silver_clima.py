"""Testes do job da silver com um Spark local, em pastas temporárias.

Precisam do pyspark e do Java instalados. Na máquina que não tiver, os testes são
pulados (e não falham): o job continua sendo testado de verdade no container.
"""

import shutil
import sys
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark", reason="pyspark não instalado")
if shutil.which("java") is None:
    pytest.skip("Java não instalado", allow_module_level=True)

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "spark" / "jobs"))

from pyspark.sql import SparkSession  # noqa: E402

import silver_clima  # noqa: E402

EXTRACAO_1 = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)
EXTRACAO_2 = datetime(2026, 10, 2, 12, 0, tzinfo=UTC)


@pytest.fixture(scope="module")
def spark():
    """Um Spark local e pequeno, compartilhado pelos testes deste arquivo."""
    sessao = (
        SparkSession.builder.master("local[1]")
        .appName("testes_silver")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    sessao.sparkContext.setLogLevel("ERROR")
    yield sessao
    sessao.stop()


def _linha_bronze(codigo, uf, dia, extraido_em=EXTRACAO_1, tmax=30.0, tmin=20.0, tmedia=25.0):
    return {
        "codigo_ibge": codigo,
        "uf_sigla": uf,
        "data": dia,
        "ano": dia.year,
        "mes": dia.month,
        "temperature_2m_max": tmax,
        "temperature_2m_min": tmin,
        "temperature_2m_mean": tmedia,
        "precipitation_sum": 0.0,
        "relative_humidity_2m_mean": 70.0,
        "wind_speed_10m_max": 10.0,
        "_fonte": "teste",
        "_extraido_em": extraido_em,
    }


def _bronze(spark, linhas):
    return spark.createDataFrame(linhas)


def _municipios(spark):
    return spark.createDataFrame(
        [
            {"codigo_ibge": 3509502, "nome": "Campinas", "uf_sigla": "SP", "regiao": "Sudeste"},
            {"codigo_ibge": 4106902, "nome": "Curitiba", "uf_sigla": "PR", "regiao": "Sul"},
        ]
    )


# --- Transformação ---
def test_transformar_padroniza_enriquece_e_calcula(spark):
    bronze = _bronze(spark, [_linha_bronze(3509502, "SP", date(2025, 1, 1))])

    linha = silver_clima.transformar(bronze, _municipios(spark)).first()

    assert linha["temp_max_c"] == 30.0
    assert linha["municipio"] == "Campinas"
    assert linha["regiao"] == "Sudeste"
    assert linha["amplitude_termica_c"] == 10.0
    assert linha["dado_completo"] is True
    assert linha["temp_suspeita"] is False


def test_deduplicacao_fica_com_a_extracao_mais_recente(spark):
    bronze = _bronze(
        spark,
        [
            _linha_bronze(3509502, "SP", date(2025, 1, 1), EXTRACAO_1, tmax=30.0),
            _linha_bronze(3509502, "SP", date(2025, 1, 1), EXTRACAO_2, tmax=31.0),
        ],
    )

    silver = silver_clima.transformar(bronze, _municipios(spark))

    assert silver.count() == 1
    assert silver.first()["temp_max_c"] == 31.0


@pytest.mark.parametrize(
    ("tmax", "tmin", "tmedia"),
    [
        (60.0, 20.0, 25.0),  # máxima fora da faixa plausível
        (20.0, 30.0, 25.0),  # mínima maior que a máxima
        (30.0, 20.0, 35.0),  # média fora do intervalo entre mínima e máxima
    ],
)
def test_temperaturas_incoerentes_sao_marcadas_e_nao_apagadas(spark, tmax, tmin, tmedia):
    linha = _linha_bronze(3509502, "SP", date(2025, 1, 1), tmax=tmax, tmin=tmin, tmedia=tmedia)

    silver = silver_clima.transformar(_bronze(spark, [linha]), _municipios(spark))

    assert silver.count() == 1
    assert silver.first()["temp_suspeita"] is True


def test_dia_com_valor_nulo_fica_incompleto_mas_nao_suspeito(spark):
    linha = _linha_bronze(3509502, "SP", date(2025, 1, 1))
    linha["temperature_2m_max"] = None
    # Com um valor nulo o Spark não consegue adivinhar o tipo da coluna, então
    # reaproveito o schema de uma linha completa.
    schema = _bronze(spark, [_linha_bronze(1, "SP", date(2025, 1, 1))]).schema
    bronze = spark.createDataFrame([linha], schema=schema)

    resultado = silver_clima.transformar(bronze, _municipios(spark)).first()

    assert resultado["dado_completo"] is False
    assert resultado["temp_suspeita"] is False


# --- Processamento completo e incremental, gravando de verdade ---
def _gravar_bronze(spark, pasta, linhas):
    _bronze(spark, linhas).write.mode("overwrite").partitionBy(*silver_clima.PARTICOES).parquet(
        str(pasta / "bronze")
    )


def _rodar(spark, pasta, modo="incremental"):
    return silver_clima.processar(
        spark,
        str(pasta / "bronze"),
        str(pasta / "municipios"),
        str(pasta / "silver"),
        modo,
    )


@pytest.fixture
def pasta(tmp_path, spark):
    _municipios(spark).write.parquet(str(tmp_path / "municipios"))
    return tmp_path


def test_primeira_execucao_incremental_vira_completa(spark, pasta):
    _gravar_bronze(
        spark,
        pasta,
        [
            _linha_bronze(3509502, "SP", date(2025, 1, 1)),
            _linha_bronze(4106902, "PR", date(2025, 1, 1)),
        ],
    )

    resumo = _rodar(spark, pasta)

    assert resumo["linhas"] == 2
    assert spark.read.parquet(str(pasta / "silver")).count() == 2


def test_incremental_sem_novidades_nao_faz_nada(spark, pasta):
    _gravar_bronze(spark, pasta, [_linha_bronze(3509502, "SP", date(2025, 1, 1))])
    _rodar(spark, pasta)

    assert _rodar(spark, pasta) == {"linhas": 0}


def test_incremental_regrava_so_as_particoes_novas(spark, pasta):
    # Carga 1: SP e PR em janeiro.
    carga_1 = [
        _linha_bronze(3509502, "SP", date(2025, 1, 1), EXTRACAO_1),
        _linha_bronze(4106902, "PR", date(2025, 1, 1), EXTRACAO_1),
    ]
    _gravar_bronze(spark, pasta, carga_1)
    _rodar(spark, pasta)

    # Carga 2: chega um dia novo de SP (extração mais recente). PR não muda.
    carga_2 = [*carga_1, _linha_bronze(3509502, "SP", date(2025, 1, 2), EXTRACAO_2)]
    _gravar_bronze(spark, pasta, carga_2)
    resumo = _rodar(spark, pasta)

    silver = spark.read.parquet(str(pasta / "silver"))
    assert resumo["linhas"] == 2  # só a partição de SP foi reprocessada
    assert silver.count() == 3  # SP (2 dias) + PR (1 dia), sem perder PR
    assert silver.filter("uf_sigla = 'PR'").count() == 1


def test_modo_completo_regrava_tudo(spark, pasta):
    _gravar_bronze(spark, pasta, [_linha_bronze(3509502, "SP", date(2025, 1, 1))])
    _rodar(spark, pasta)

    resumo = _rodar(spark, pasta, modo="completo")

    assert resumo["linhas"] == 1
    assert spark.read.parquet(str(pasta / "silver")).count() == 1
