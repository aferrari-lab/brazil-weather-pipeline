"""Leva o log das cargas (que fica no lake) para o Postgres, onde vira painel.

Cada carga da ingestão grava uma linha em s3://lake/controle/log_cargas/. Aqui eu
copio esse histórico para a tabela ops.log_cargas, que o dbt transforma no modelo
obs_cargas e o Metabase mostra no painel de saúde.

A cópia é um upsert pelo id da execução: rodar de novo não duplica nada, e uma
execução que mudou de status (ex.: estava em andamento) é atualizada.
"""

import math
from datetime import datetime

import pandas as pd

TABELA_LOG = "ops.log_cargas"

COLUNAS_LOG = [
    "id_execucao",
    "processo",
    "status",
    "inicio",
    "fim",
    "duracao_segundos",
    "linhas_novas",
    "chamadas_api",
    "parametros",
    "mensagem_erro",
]

SQL_CRIAR_TABELA = [
    "CREATE SCHEMA IF NOT EXISTS ops",
    f"""
    CREATE TABLE IF NOT EXISTS {TABELA_LOG} (
        id_execucao       text PRIMARY KEY,
        processo          text,
        status            text,
        inicio            timestamptz,
        fim               timestamptz,
        duracao_segundos  double precision,
        linhas_novas      bigint,
        chamadas_api      bigint,
        parametros        text,
        mensagem_erro     text
    )
    """,
]


def _valor_sql(valor):
    """Converte os vazios do pandas (NaN, NaT, <NA>) em None, que vira NULL no banco."""
    if valor is None or valor is pd.NaT or valor is pd.NA:
        return None
    if isinstance(valor, float) and math.isnan(valor):
        return None
    if isinstance(valor, pd.Timestamp):
        return valor.to_pydatetime()
    if hasattr(valor, "item"):  # tipos do numpy (int64, float64) → tipos do Python
        return valor.item()
    return valor


def linhas_do_log(log: pd.DataFrame) -> list[tuple]:
    """Transforma o log em tuplas na ordem de COLUNAS_LOG, prontas para o INSERT."""
    if log.empty:
        return []
    return [
        tuple(_valor_sql(valor) for valor in linha)
        for linha in log[COLUNAS_LOG].itertuples(index=False)
    ]


def publicar_log(hook, log: pd.DataFrame) -> int:
    """Grava o log no Postgres (upsert por id_execucao). Devolve as linhas enviadas."""
    hook.run(SQL_CRIAR_TABELA)
    linhas = linhas_do_log(log)
    if linhas:
        hook.insert_rows(
            TABELA_LOG,
            linhas,
            target_fields=COLUNAS_LOG,
            replace=True,
            replace_index="id_execucao",
        )
    return len(linhas)


def tarefa_publicar_log_cargas():
    """Tarefa do Airflow que publica o log. Roda mesmo se a ingestão falhar
    (trigger_rule all_done): o registro de uma carga com erro é o mais útil."""
    from airflow.sdk import task

    @task(task_id="publicar_log_cargas", trigger_rule="all_done", retries=1)
    def publicar_log_cargas() -> int:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        from ingestion.lake import carregar_config
        from ingestion.log_cargas import ler_log

        log = ler_log(carregar_config(), ultimas=None)
        enviadas = publicar_log(PostgresHook(postgres_conn_id="warehouse"), log)
        print(f"{enviadas} execução(ões) publicadas em {TABELA_LOG} às {datetime.now():%H:%M}.")
        return enviadas

    return publicar_log_cargas()
