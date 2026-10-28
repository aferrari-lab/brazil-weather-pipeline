"""Testes da configuração do Spark, sem precisar do Spark instalado.

A montagem das configurações é uma função pura em spark/jobs/comum.py, então eu
a testo direto, passando um ambiente de mentira em vez das variáveis reais.
"""

import sys
from pathlib import Path

import pytest

# Os jobs Spark ficam em spark/jobs (e rodam de lá dentro do container); aqui eu
# coloco essa pasta no caminho de importação para conseguir importar o comum.py.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "spark" / "jobs"))

import comum  # noqa: E402

AMBIENTE = {
    "LAKE_ACCESS_KEY": "chave",
    "LAKE_SECRET_KEY": "segredo",
    "LAKE_BUCKET": "lake",
    "S3_ENDPOINT_URL": "http://lake:9000",
}


def test_config_s3a_aponta_para_o_lake():
    config = comum.montar_config_s3a(AMBIENTE)

    assert config["spark.hadoop.fs.s3a.endpoint"] == "http://lake:9000"
    assert config["spark.hadoop.fs.s3a.access.key"] == "chave"
    assert config["spark.hadoop.fs.s3a.path.style.access"] == "true"
    assert config["spark.hadoop.fs.s3a.connection.ssl.enabled"] == "false"
    assert config["spark.sql.session.timeZone"] == "UTC"


def test_config_s3a_liga_ssl_com_https():
    ambiente = {**AMBIENTE, "S3_ENDPOINT_URL": "https://s3.amazonaws.com"}

    assert comum.montar_config_s3a(ambiente)["spark.hadoop.fs.s3a.connection.ssl.enabled"] == (
        "true"
    )


def test_config_s3a_falha_sem_credenciais():
    with pytest.raises(OSError, match="LAKE_SECRET_KEY"):
        comum.montar_config_s3a({"LAKE_ACCESS_KEY": "chave"})


def test_caminho_lake_usa_prefixo_s3a():
    assert comum.caminho_lake("/bronze/clima_diario/", AMBIENTE) == "s3a://lake/bronze/clima_diario"
