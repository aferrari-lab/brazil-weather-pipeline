"""Testes do módulo do lake, sem precisar do RustFS rodando.

Eu aponto o lake para uma pasta temporária local: o código de gravação e leitura
é o mesmo, só muda a raiz. Assim testo partições e idempotência offline.
"""

import pandas as pd
import pytest

from ingestion.lake import (
    ConfigLake,
    caminho_completo,
    carregar_config,
    gravar_parquet,
    ler_parquet,
)


@pytest.fixture
def lake_local(tmp_path) -> ConfigLake:
    """Um lake de mentira numa pasta temporária, apagada ao fim do teste."""
    return ConfigLake(raiz=str(tmp_path / "lake"))


def _clima(uf: str, mes: int, temperaturas: list[float]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "uf": uf,
            "ano": 2026,
            "mes": mes,
            "dia": range(1, len(temperaturas) + 1),
            "temp": temperaturas,
        }
    )


def test_caminho_completo_junta_raiz_e_caminho():
    config = ConfigLake(raiz="s3://lake")
    assert caminho_completo("/bronze/municipios/", config) == "s3://lake/bronze/municipios"


@pytest.mark.parametrize("caminho", ["", "/", "bronze/../segredos"])
def test_caminho_completo_recusa_caminho_invalido(caminho):
    with pytest.raises(ValueError, match="Caminho inválido"):
        caminho_completo(caminho, ConfigLake(raiz="s3://lake"))


def test_grava_e_le_arquivo_unico(lake_local):
    df = pd.DataFrame({"codigo_ibge": [1, 2], "nome": ["A", "B"]})

    gravar_parquet(df, "bronze/teste/tabela.parquet", lake_local)
    lido = ler_parquet("bronze/teste/tabela.parquet", lake_local)

    pd.testing.assert_frame_equal(lido, df)


def test_arquivo_unico_sobrescreve_ao_gravar_de_novo(lake_local):
    gravar_parquet(pd.DataFrame({"x": [1, 2, 3]}), "bronze/t.parquet", lake_local)
    gravar_parquet(pd.DataFrame({"x": [9]}), "bronze/t.parquet", lake_local)

    assert ler_parquet("bronze/t.parquet", lake_local)["x"].tolist() == [9]


def test_particoes_sao_idempotentes(lake_local):
    # Gravo a mesma partição duas vezes: o resultado não pode ter linhas duplicadas.
    gravar_parquet(_clima("SP", 10, [20.0, 21.0]), "bronze/clima", lake_local, ["ano", "mes", "uf"])
    gravar_parquet(_clima("SP", 10, [22.0, 23.0]), "bronze/clima", lake_local, ["ano", "mes", "uf"])

    lido = ler_parquet("bronze/clima", lake_local)
    assert len(lido) == 2
    assert sorted(lido["temp"]) == [22.0, 23.0]


def test_particoes_preservam_as_outras(lake_local):
    # Regravar SP não pode apagar os dados do PR.
    gravar_parquet(_clima("PR", 10, [15.0]), "bronze/clima", lake_local, ["ano", "mes", "uf"])
    gravar_parquet(_clima("SP", 10, [25.0]), "bronze/clima", lake_local, ["ano", "mes", "uf"])
    gravar_parquet(_clima("SP", 10, [26.0]), "bronze/clima", lake_local, ["ano", "mes", "uf"])

    lido = ler_parquet("bronze/clima", lake_local)
    assert sorted(lido["uf"].astype(str)) == ["PR", "SP"]


def test_recusa_dataframe_vazio(lake_local):
    with pytest.raises(ValueError, match="vazio"):
        gravar_parquet(pd.DataFrame(), "bronze/vazio.parquet", lake_local)


def test_recusa_particao_inexistente(lake_local):
    with pytest.raises(ValueError, match="partição ausentes"):
        gravar_parquet(_clima("SP", 10, [1.0]), "bronze/clima", lake_local, ["regiao"])


@pytest.fixture
def sem_dotenv(monkeypatch):
    """Desliga a leitura do .env durante o teste.

    O load_dotenv procura o .env a partir da pasta do módulo (ingestion/), subindo
    até a raiz do projeto. Sem isso, os testes leriam o meu .env real e o resultado
    dependeria da máquina onde rodam.
    """
    monkeypatch.setattr("ingestion.lake.load_dotenv", lambda *args, **kwargs: None)


def test_config_falha_sem_variaveis(monkeypatch, sem_dotenv):
    for nome in ["LAKE_ACCESS_KEY", "LAKE_SECRET_KEY", "LAKE_BUCKET"]:
        monkeypatch.delenv(nome, raising=False)

    with pytest.raises(OSError, match="LAKE_ACCESS_KEY"):
        carregar_config()


def test_config_monta_acesso_s3(monkeypatch, sem_dotenv):
    monkeypatch.setenv("LAKE_ACCESS_KEY", "chave")
    monkeypatch.setenv("LAKE_SECRET_KEY", "segredo")
    monkeypatch.setenv("LAKE_BUCKET", "lake")
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://lake:9000")

    config = carregar_config()

    assert config.raiz == "s3://lake"
    assert config.storage_options["client_kwargs"]["endpoint_url"] == "http://lake:9000"
    assert config.storage_options["config_kwargs"]["s3"]["addressing_style"] == "path"
