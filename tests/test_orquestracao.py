"""Testes da orquestração: carga do Brasil, tarefas em container, alertas e DAGs.

Os testes das DAGs precisam do Airflow instalado e são pulados sem ele (no Windows
o Airflow roda só no Docker); o resto roda em qualquer lugar.
"""

from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest
import requests

from ingestion import clima
from ingestion.lake import ConfigLake
from orquestracao import alertas, containers, observabilidade

CONFIG = ConfigLake(raiz="memory://lake", storage_options={})


# --- Carga do Brasil até o limite da API ---
def test_ordenar_ufs_poe_prioritarias_na_frente_e_resto_em_ordem():
    assert clima.ordenar_ufs(["rj", "SP", "AC", "MG", "RJ"]) == ["SP", "AC", "MG", "RJ"]


def test_ordenar_ufs_ignora_prioritaria_ausente():
    assert clima.ordenar_ufs(["RJ", "AC"], prioridade=("SP",)) == ["AC", "RJ"]


@pytest.fixture
def municipios_falsos(monkeypatch):
    tabela = pd.DataFrame({"uf_sigla": ["RJ", "SP", "SP", "AC"]})
    monkeypatch.setattr(clima, "ler_parquet", lambda *args, **kwargs: tabela)


def test_carga_brasil_completa_devolve_sucesso(monkeypatch, municipios_falsos):
    chamadas = []
    monkeypatch.setattr(
        clima,
        "carregar_clima",
        lambda ufs, inicio, fim, **kw: chamadas.append((ufs, inicio, fim, kw["limite_e_erro"])),
    )

    status = clima.carregar_clima_brasil(date(2026, 10, 20), config=CONFIG)

    assert status == "sucesso"
    assert chamadas == [(["SP", "AC", "RJ"], clima.DATA_INICIO, date(2026, 10, 20), False)]


def test_carga_brasil_no_limite_da_api_nao_falha(monkeypatch, municipios_falsos):
    def estoura_limite(*args, **kwargs):
        raise requests.exceptions.RetryError("too many 429 error responses")

    monkeypatch.setattr(clima, "carregar_clima", estoura_limite)

    assert clima.carregar_clima_brasil(date(2026, 10, 20), config=CONFIG) == "limite_api"


def test_carga_brasil_deixa_passar_erro_de_verdade(monkeypatch, municipios_falsos):
    def quebra(*args, **kwargs):
        raise clima.RespostaInvalidaError("resposta sem JSON")

    monkeypatch.setattr(clima, "carregar_clima", quebra)

    with pytest.raises(clima.RespostaInvalidaError):
        clima.carregar_clima_brasil(date(2026, 10, 20), config=CONFIG)


# --- Tarefas em container ---
def test_ambiente_dos_jobs_repassa_credenciais_e_troca_enderecos():
    ambiente = {
        "LAKE_ACCESS_KEY": "chave",
        "LAKE_SECRET_KEY": "segredo",
        "LAKE_BUCKET": "lake",
        "WAREHOUSE_USER": "dw",
        "WAREHOUSE_PASSWORD": "senha",
        "WAREHOUSE_DB": "warehouse",
        "S3_ENDPOINT_URL": "http://localhost:9000",  # o de fora, que não serve no container
        "AIRFLOW_JWT_SECRET": "nao-deve-vazar",
    }

    resultado = containers.ambiente_dos_jobs(ambiente)

    assert resultado["LAKE_SECRET_KEY"] == "segredo"
    assert resultado["S3_ENDPOINT_URL"] == "http://lake:9000"
    assert resultado["WAREHOUSE_HOST"] == "postgres"
    assert "AIRFLOW_JWT_SECRET" not in resultado


def test_comando_spark_usa_spark_submit():
    assert containers.comando_spark("silver_clima.py", "--modo", "completo") == [
        "/opt/spark/bin/spark-submit",
        "--driver-memory",
        "2g",
        "silver_clima.py",
        "--modo",
        "completo",
    ]


# --- Alertas ---
def _contexto():
    ti = SimpleNamespace(dag_id="clima_diario", task_id="dbt_build", try_number=2, run_id="r1")
    return {"ti": ti, "exception": ValueError("teste falhou")}


def test_mensagem_de_falha_traz_o_essencial():
    mensagem = alertas.montar_mensagem(_contexto())

    assert "clima_diario" in mensagem
    assert "dbt_build (tentativa 2)" in mensagem
    assert "teste falhou" in mensagem


def test_sem_telegram_configurado_nao_envia(monkeypatch):
    monkeypatch.setattr(alertas.requests, "post", lambda *a, **k: pytest.fail("não devia enviar"))

    assert alertas.enviar_telegram("oi", ambiente={}) is False


def test_falha_do_telegram_nao_derruba_o_callback(monkeypatch):
    def telegram_fora(*args, **kwargs):
        raise requests.ConnectionError("sem rede")

    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "x")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "y")
    monkeypatch.setattr(alertas.requests, "post", telegram_fora)

    alertas.avisar_falha(_contexto())  # não pode levantar exceção


# --- DAGs (só com o Airflow instalado) ---
@pytest.fixture(scope="module")
def dagbag():
    pytest.importorskip("airflow.providers.docker", reason="Airflow não instalado")
    from pathlib import Path

    from airflow.models.dagbag import DagBag

    pasta = Path(__file__).resolve().parents[1] / "airflow" / "dags"
    return DagBag(dag_folder=str(pasta), include_examples=False)


def test_dags_carregam_sem_erro(dagbag):
    assert dagbag.import_errors == {}
    assert {"smoke_test", "clima_diario", "clima_reprocessar"} <= set(dagbag.dag_ids)


@pytest.mark.parametrize(
    ("dag_id", "primeira"),
    [("clima_diario", "ingerir_clima"), ("clima_reprocessar", "reprocessar_clima")],
)
def test_ordem_das_tarefas(dagbag, dag_id, primeira):
    dag = dagbag.dags[dag_id]  # direto do arquivo, sem consultar o banco
    ordem = [primeira, "silver_clima", "silver_postgres", "dbt_build"]

    for anterior, seguinte in zip(ordem, ordem[1:], strict=False):
        assert seguinte in dag.get_task(anterior).downstream_task_ids


def test_dag_diaria_nao_faz_catchup_e_roda_uma_por_vez(dagbag):
    dag = dagbag.dags["clima_diario"]

    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.get_task("ingerir_clima").pool == "api_open_meteo"
    assert dag.get_task("dbt_build").pool == "transformacao"


# --- Log de cargas no Postgres ---
class HookFalso:
    """Registra o que seria mandado ao Postgres, sem banco."""

    def __init__(self):
        self.comandos, self.inseridos = [], []

    def run(self, sql):
        self.comandos.extend(sql)

    def insert_rows(self, tabela, linhas, target_fields, replace, replace_index):
        self.inseridos.append((tabela, linhas, target_fields, replace, replace_index))


def _log_exemplo():
    log = pd.DataFrame(
        {
            "id_execucao": ["a1", "a2"],
            "processo": ["clima", "clima"],
            "status": ["sucesso", "erro"],
            "inicio": pd.to_datetime(["2026-10-28 09:00", "2026-10-29 09:00"], utc=True),
            "fim": pd.to_datetime(["2026-10-28 09:30", None], utc=True),
            "duracao_segundos": [1800.0, float("nan")],
            "linhas_novas": pd.array([640, None], dtype="Int64"),
            "chamadas_api": pd.array([2, 0], dtype="Int64"),
            "parametros": ['{"ufs": ["SP"]}', None],
            "mensagem_erro": [None, "lake fora do ar"],
        }
    )
    return log


def test_linhas_do_log_trocam_vazios_do_pandas_por_none():
    linhas = observabilidade.linhas_do_log(_log_exemplo())

    assert len(linhas) == 2
    segunda = dict(zip(observabilidade.COLUNAS_LOG, linhas[1], strict=True))
    assert segunda["fim"] is None
    assert segunda["duracao_segundos"] is None
    assert segunda["linhas_novas"] is None
    assert isinstance(
        dict(zip(observabilidade.COLUNAS_LOG, linhas[0], strict=True))["linhas_novas"], int
    )


def test_publicar_log_cria_tabela_e_faz_upsert_por_execucao():
    hook = HookFalso()

    enviadas = observabilidade.publicar_log(hook, _log_exemplo())

    assert enviadas == 2
    assert any("CREATE TABLE IF NOT EXISTS ops.log_cargas" in c for c in hook.comandos)
    tabela, _, colunas, replace, indice = hook.inseridos[0]
    assert (tabela, replace, indice) == ("ops.log_cargas", True, "id_execucao")
    assert colunas == observabilidade.COLUNAS_LOG


def test_publicar_log_vazio_so_garante_a_tabela():
    hook = HookFalso()

    assert (
        observabilidade.publicar_log(hook, pd.DataFrame(columns=observabilidade.COLUNAS_LOG)) == 0
    )
    assert hook.inseridos == []


def test_log_publicado_antes_do_dbt_e_mesmo_se_a_ingestao_falhar(dagbag):
    for dag_id in ("clima_diario", "clima_reprocessar"):
        tarefa = dagbag.dags[dag_id].get_task("publicar_log_cargas")

        assert tarefa.trigger_rule == "all_done"
        assert "dbt_build" in tarefa.downstream_task_ids
