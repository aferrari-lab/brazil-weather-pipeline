"""Testes do log de execuções, num lake local temporário."""

import json
from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest
import requests

from ingestion import clima, log_cargas
from ingestion.lake import ConfigLake, gravar_parquet, ler_parquet
from ingestion.municipios import CAMINHO_MUNICIPIOS
from tests.test_clima import FIM, INICIO, MUNICIPIOS, _SessaoFalsa


@pytest.fixture
def lake_local(tmp_path) -> ConfigLake:
    return ConfigLake(raiz=str(tmp_path / "lake"))


def _log(lake: ConfigLake) -> pd.DataFrame:
    return ler_parquet(log_cargas.CAMINHO_LOG, lake)


# --- Partes puras ---
@pytest.mark.parametrize(
    ("erro", "status"),
    [
        (None, "sucesso"),
        (requests.exceptions.RetryError("tentativas esgotadas"), "limite_api"),
        (ValueError("dado ruim"), "erro"),
        (KeyboardInterrupt(), "interrompida"),
    ],
)
def test_classificar_erro(erro, status):
    assert log_cargas.classificar_erro(erro) == status


def test_registro_tem_duracao_parametros_e_mensagem_curta():
    execucao = log_cargas.Execucao(processo="clima", parametros={"ufs": ["SP"]})
    fim = execucao.inicio + timedelta(seconds=90)

    registro = log_cargas.montar_registro(execucao, ValueError("x" * 1000), fim).iloc[0]

    assert registro["duracao_segundos"] == 90.0
    assert json.loads(registro["parametros"]) == {"ufs": ["SP"]}
    assert len(registro["mensagem_erro"]) == log_cargas.TAMANHO_MAXIMO_MENSAGEM


# --- Context manager ---
def test_execucao_com_sucesso_fica_registrada(lake_local):
    with log_cargas.registrar_carga("teste", {"a": 1}, lake_local) as execucao:
        execucao.linhas_novas = 10
        execucao.chamadas_api = 2

    linha = _log(lake_local).iloc[0]
    assert linha["status"] == "sucesso"
    assert (linha["linhas_novas"], linha["chamadas_api"]) == (10, 2)
    assert pd.isna(linha["mensagem_erro"])


def test_execucao_com_erro_fica_registrada_e_o_erro_continua_subindo(lake_local):
    with (
        pytest.raises(ValueError, match="quebrou"),
        log_cargas.registrar_carga("teste", {}, lake_local) as execucao,
    ):
        execucao.chamadas_api = 1
        raise ValueError("quebrou")

    linha = _log(lake_local).iloc[0]
    assert linha["status"] == "erro"
    assert "quebrou" in linha["mensagem_erro"]
    assert linha["chamadas_api"] == 1


def test_limite_da_api_tem_status_proprio(lake_local):
    with (
        pytest.raises(requests.exceptions.RetryError),
        log_cargas.registrar_carga("teste", {}, lake_local),
    ):
        raise requests.exceptions.RetryError("429")

    assert _log(lake_local).iloc[0]["status"] == "limite_api"


def test_cada_execucao_grava_seu_proprio_registro(lake_local):
    for _ in range(3):
        with log_cargas.registrar_carga("teste", {}, lake_local):
            pass

    log = _log(lake_local)
    assert len(log) == 3
    assert log["id_execucao"].is_unique


def test_falha_ao_gravar_o_log_nao_esconde_o_erro_da_carga(lake_local, monkeypatch):
    def gravar_quebrado(*args, **kwargs):
        raise OSError("lake fora do ar")

    monkeypatch.setattr(log_cargas, "gravar_parquet", gravar_quebrado)

    # O erro que chega a quem chamou é o da carga, não o do log.
    with (
        pytest.raises(ValueError, match="erro da carga"),
        log_cargas.registrar_carga("teste", {}, lake_local),
    ):
        raise ValueError("erro da carga")


# --- Integração com a carga de clima ---
def test_carga_de_clima_registra_linhas_e_chamadas(lake_local):
    gravar_parquet(MUNICIPIOS, CAMINHO_MUNICIPIOS, lake_local)

    clima.carregar_clima(["SP"], INICIO, FIM, config=lake_local, sessao=_SessaoFalsa())

    linha = _log(lake_local).iloc[0]
    assert linha["processo"] == "clima"
    assert linha["status"] == "sucesso"
    assert linha["linhas_novas"] == 2 * 4  # 2 cidades de SP × 4 dias
    assert linha["chamadas_api"] == 1  # as 2 cidades cabem em um lote
    assert json.loads(linha["parametros"])["ufs"] == ["SP"]
    assert linha["fim"] >= linha["inicio"] >= pd.Timestamp(datetime(2026, 1, 1, tzinfo=UTC))


def test_ler_log_traz_as_mais_recentes_primeiro(lake_local):
    assert log_cargas.ler_log(lake_local).empty  # ainda sem execuções

    for processo in ["primeira", "segunda"]:
        with log_cargas.registrar_carga(processo, {}, lake_local):
            pass

    assert log_cargas.ler_log(lake_local)["processo"].tolist() == ["segunda", "primeira"]


def test_ler_log_com_execucoes_de_sucesso_e_de_erro(lake_local):
    # O cenário que quebrava: um arquivo com "mensagem_erro" vazia e outro com texto.
    with log_cargas.registrar_carga("ok", {}, lake_local):
        pass
    with pytest.raises(ValueError), log_cargas.registrar_carga("falhou", {}, lake_local):
        raise ValueError("deu ruim")

    log = log_cargas.ler_log(lake_local)

    assert set(log["status"]) == {"sucesso", "erro"}
    assert log.loc[log["processo"] == "falhou", "mensagem_erro"].item().endswith("deu ruim")


def test_ler_log_aceita_arquivo_antigo_com_coluna_toda_nula(lake_local):
    # Simulo um arquivo gravado antes da correção: mensagem_erro com tipo "nulo".
    with log_cargas.registrar_carga("antigo", {}, lake_local) as execucao:
        pass
    antigo = log_cargas.montar_registro(execucao, None, execucao.inicio)
    antigo["mensagem_erro"] = None
    antigo = antigo.astype({"mensagem_erro": "object"})
    gravar_parquet(antigo, f"{log_cargas.CAMINHO_LOG}/00000000_antigo.parquet", lake_local)
    with pytest.raises(ValueError), log_cargas.registrar_carga("novo", {}, lake_local):
        raise ValueError("erro")

    assert len(log_cargas.ler_log(lake_local)) == 3


def test_interrupcao_manual_fica_registrada_como_interrompida(lake_local):
    with pytest.raises(KeyboardInterrupt), log_cargas.registrar_carga("teste", {}, lake_local):
        raise KeyboardInterrupt

    linha = log_cargas.ler_log(lake_local).iloc[0]
    assert linha["status"] == "interrompida"
    assert linha["mensagem_erro"] == "KeyboardInterrupt"
