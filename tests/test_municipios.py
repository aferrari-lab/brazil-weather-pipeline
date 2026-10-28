"""Testes da tabela de municípios, sem acessar a internet.

Eu testo a transformação e a validação com um JSON de exemplo no formato da API
do IBGE. Assim os testes são rápidos, rodam offline e não dependem da API estar no ar.
"""

from datetime import UTC, datetime

import pandas as pd
import pytest

from ingestion.lake import ConfigLake, ler_parquet
from ingestion.municipios import (
    CAMINHO_MUNICIPIOS,
    combinar_municipios,
    salvar_municipios,
    transformar_coordenadas,
    transformar_municipios_ibge,
    validar_coordenadas,
    validar_municipios,
    validar_municipios_ibge,
)


def _municipio(codigo: int, nome: str, uf: str, regiao: str) -> dict:
    """Monta um município no mesmo formato aninhado que a API do IBGE devolve."""
    return {
        "id": codigo,
        "nome": nome,
        "microrregiao": None,  # de propósito: o código não pode depender deste campo
        "regiao-imediata": {
            "regiao-intermediaria": {
                "UF": {"sigla": uf, "regiao": {"nome": regiao}},
            },
        },
    }


EXTRAIDO_EM = datetime(2026, 10, 28, 12, 0, tzinfo=UTC)
DADOS_EXEMPLO = [
    _municipio(3509502, "Campinas", "SP", "Sudeste"),
    _municipio(4106902, "Curitiba", "PR", "Sul"),
]


def test_transformacao_gera_colunas_e_tipos_esperados():
    df = transformar_municipios_ibge(DADOS_EXEMPLO, EXTRAIDO_EM)

    assert list(df.columns) == [
        "codigo_ibge",
        "nome",
        "uf_sigla",
        "regiao",
        "_fonte",
        "_extraido_em",
    ]
    assert df["codigo_ibge"].dtype == "int64"
    assert df.loc[0, "uf_sigla"] == "SP"
    assert df.loc[1, "regiao"] == "Sul"


def test_transformacao_falha_se_o_formato_mudar():
    dados_sem_regiao = [{"id": 1, "nome": "Sem região"}]

    with pytest.raises(ValueError, match="colunas esperadas"):
        transformar_municipios_ibge(dados_sem_regiao, EXTRAIDO_EM)


def test_validacao_barra_codigo_duplicado():
    df = transformar_municipios_ibge(DADOS_EXEMPLO + DADOS_EXEMPLO[:1], EXTRAIDO_EM)

    with pytest.raises(ValueError, match="duplicados"):
        validar_municipios_ibge(df)


def test_validacao_aceita_tabela_correta():
    df = transformar_municipios_ibge(DADOS_EXEMPLO, EXTRAIDO_EM)

    validar_municipios_ibge(df)  # não deve levantar exceção


# --- Coordenadas ---
# Um CSV pequeno no mesmo formato do arquivo real, com uma coluna extra ("nome")
# que a transformação deve descartar.
CSV_EXEMPLO = (
    "codigo_ibge,nome,latitude,longitude\n"
    "3509502,Campinas,-22.9053,-47.0659\n"
    "4106902,Curitiba,-25.4195,-49.2646\n"
)


def test_coordenadas_gera_colunas_e_tipos_esperados():
    df = transformar_coordenadas(CSV_EXEMPLO, EXTRAIDO_EM)

    assert list(df.columns) == ["codigo_ibge", "latitude", "longitude", "_fonte", "_extraido_em"]
    assert df["codigo_ibge"].dtype == "int64"
    assert df["latitude"].dtype == "float64"


def test_coordenadas_aceita_tabela_correta():
    validar_coordenadas(transformar_coordenadas(CSV_EXEMPLO, EXTRAIDO_EM))


def test_coordenadas_barra_ponto_fora_do_brasil():
    # Paris: latitude positiva bem acima do limite norte do Brasil.
    csv_com_paris = CSV_EXEMPLO + "9999999,Paris,48.8566,2.3522\n"
    df = transformar_coordenadas(csv_com_paris, EXTRAIDO_EM)

    with pytest.raises(ValueError, match="fora do Brasil"):
        validar_coordenadas(df)


def test_coordenadas_falha_se_o_formato_mudar():
    with pytest.raises(ValueError, match="colunas esperadas"):
        transformar_coordenadas("codigo,lat,lon\n1,-22.9,-47.0\n", EXTRAIDO_EM)


# --- Tabela final (IBGE + coordenadas) ---
def _tabelas_exemplo(csv: str = CSV_EXEMPLO):
    municipios = transformar_municipios_ibge(DADOS_EXEMPLO, EXTRAIDO_EM)
    coordenadas = transformar_coordenadas(csv, EXTRAIDO_EM)
    return municipios, coordenadas


def test_combinar_junta_as_duas_fontes():
    df = combinar_municipios(*_tabelas_exemplo())

    assert len(df) == 2
    assert df["latitude"].notna().all()
    assert df.loc[df["nome"] == "Campinas", "uf_sigla"].item() == "SP"


def test_combinar_mantem_municipio_sem_coordenada():
    # Só Campinas tem coordenada: Curitiba precisa continuar na tabela, sem lat/long.
    csv_so_campinas = "codigo_ibge,latitude,longitude\n3509502,-22.9053,-47.0659\n"
    df = combinar_municipios(*_tabelas_exemplo(csv_so_campinas))

    assert len(df) == 2
    assert pd.isna(df.loc[df["nome"] == "Curitiba", "latitude"].item())


def test_combinar_descarta_coordenada_sem_municipio():
    csv_com_orfa = CSV_EXEMPLO + "1111111,Inexistente,-10.0,-50.0\n"
    df = combinar_municipios(*_tabelas_exemplo(csv_com_orfa))

    assert 1111111 not in df["codigo_ibge"].tolist()
    assert len(df) == 2


def test_validacao_final_barra_cobertura_baixa():
    csv_so_campinas = "codigo_ibge,latitude,longitude\n3509502,-22.9053,-47.0659\n"
    df = combinar_municipios(*_tabelas_exemplo(csv_so_campinas))

    with pytest.raises(ValueError, match="Cobertura"):
        validar_municipios(df)


def test_salvar_grava_no_lake_e_le_de_volta(tmp_path):
    # Aponto o lake para uma pasta temporária, que o pytest cria e apaga sozinho.
    lake_local = ConfigLake(raiz=str(tmp_path / "lake"))
    df = combinar_municipios(*_tabelas_exemplo())

    salvar_municipios(df, lake_local)
    lido = ler_parquet(CAMINHO_MUNICIPIOS, lake_local)

    assert lido["codigo_ibge"].tolist() == df["codigo_ibge"].tolist()
