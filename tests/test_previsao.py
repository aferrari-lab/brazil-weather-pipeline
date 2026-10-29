"""Testes do job de previsão com uma série sintética, sem banco.

A série imita temperatura de verdade: um ciclo anual mais um "tempo" que persiste
por alguns dias (uma frente fria esfria vários dias seguidos). Assim os palpites
de referência erram como no mundo real, e dá para conferir a avaliação.
"""

import sys
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("sklearn", reason="scikit-learn não instalado (o job roda na imagem ml)")
pytest.importorskip("psycopg", reason="psycopg não instalado (o job roda na imagem ml)")
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ml"))

import previsao  # noqa: E402

AGORA = datetime(2026, 10, 29, 9, 0, tzinfo=UTC)


def _serie_sintetica(municipios=(1, 2, 3), dias=500, semente=0) -> pd.DataFrame:
    """Monta as features como o dbt monta, a partir de uma série inventada."""
    gerador = np.random.default_rng(semente)
    datas = pd.date_range("2025-01-01", periods=dias, freq="D")
    linhas = []
    for indice, codigo in enumerate(municipios):
        dia_do_ano = datas.dayofyear.to_numpy()
        ciclo = 22 + indice + 4 * np.cos(2 * np.pi * (dia_do_ano - 15) / 365.25)
        tempo = np.zeros(dias)
        for t in range(1, dias):  # AR(1): o desvio de hoje puxa o de amanhã
            tempo[t] = 0.7 * tempo[t - 1] + gerador.normal(0, 1.5)
        temp = ciclo + tempo
        df = pd.DataFrame({"codigo_ibge": codigo, "data": datas, "temp": temp})
        for atraso in (1, 2, 3, 7):
            df[f"temp_media_d{atraso}"] = df["temp"].shift(atraso)
        df["temp_media_7d_anteriores"] = df["temp"].shift(1).rolling(7).mean()
        df["temp_max_d1"] = df["temp_media_d1"] + 5
        df["temp_min_d1"] = df["temp_media_d1"] - 5
        df["chuva_d1"] = 0.0
        df["umidade_d1"] = 70.0
        df["vento_d1"] = 10.0
        df["sazonal_seno"] = np.sin(2 * np.pi * dia_do_ano / 365.25)
        df["sazonal_cosseno"] = np.cos(2 * np.pi * dia_do_ano / 365.25)
        df["latitude"] = -20.0 - indice
        df["longitude"] = -45.0
        df["mes"] = df["data"].dt.month
        df[previsao.ALVO] = df["temp"]
        df["conjunto"] = "historico"
        # A linha do dia seguinte, sem medição, como no modelo do dbt.
        futuro = df.iloc[[-1]].copy()
        futuro["data"] = futuro["data"] + pd.Timedelta(days=1)
        futuro[previsao.ALVO] = np.nan
        futuro["conjunto"] = "futuro"
        futuro["temp_media_d1"] = df["temp"].iloc[-1]
        linhas.append(pd.concat([df.dropna(subset=["temp_media_d7"]), futuro]))
    return pd.concat(linhas, ignore_index=True).drop(columns="temp")


@pytest.fixture(scope="module")
def resultado():
    return previsao.executar(_serie_sintetica(), AGORA)


def test_teste_fica_todo_depois_do_treino():
    df = _serie_sintetica().assign(data=lambda d: pd.to_datetime(d["data"]))
    treino, teste, futuro = previsao.separar_conjuntos(df, dias_teste=60)

    assert treino["data"].max() < teste["data"].min()
    assert teste["data"].nunique() == 60
    assert (futuro["conjunto"] == "futuro").all()


def test_historico_curto_demais_da_erro_claro():
    df = _serie_sintetica(dias=40).assign(data=lambda d: pd.to_datetime(d["data"]))

    with pytest.raises(ValueError, match="curto demais"):
        previsao.separar_conjuntos(df, dias_teste=60)


def test_avalia_os_tres_palpites(resultado):
    assert set(resultado.avaliacao["modelo"]) == {
        "persistencia",
        "media_sazonal",
        "gradient_boosting",
    }
    assert (resultado.avaliacao["mae_c"] > 0).all()
    assert (resultado.avaliacao["rmse_c"] >= resultado.avaliacao["mae_c"]).all()


def test_modelo_aprende_mais_que_a_media_sazonal(resultado):
    # Com "tempo" que persiste, saber a véspera ajuda muito: o modelo tem que
    # errar menos que quem só olha a média do mês.
    erros = resultado.avaliacao.set_index("modelo")["mae_c"]
    assert erros["gradient_boosting"] < erros["media_sazonal"]


def test_preve_um_dia_a_frente_para_cada_municipio(resultado):
    futuras = resultado.previsoes_futuras

    assert sorted(futuras["codigo_ibge"]) == [1, 2, 3]
    assert futuras["previsto_c"].notna().all()
    assert (futuras["versao"] == previsao.VERSAO_MODELO).all()


def test_media_sazonal_sem_o_mes_no_treino_usa_os_7_dias_anteriores():
    sazonal = pd.DataFrame({"codigo_ibge": [1], "mes": [1], "media_sazonal": [25.0]})
    df = pd.DataFrame(
        {"codigo_ibge": [1, 1], "mes": [1, 2], "temp_media_7d_anteriores": [20.0, 18.0]}
    )

    assert previsao.prever_sazonal(sazonal, df).tolist() == [25.0, 18.0]


def test_linhas_para_o_banco_usam_tipos_do_python():
    df = pd.DataFrame({"a": np.array([1, 2], dtype="int64"), "b": [1.5, np.nan]})

    linhas = previsao._linhas(df)

    assert linhas == [(1, 1.5), (2, None)]
    assert type(linhas[0][0]) is int
