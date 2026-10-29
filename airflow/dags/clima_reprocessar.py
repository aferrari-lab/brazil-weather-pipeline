"""Reprocessa um período do clima, sob demanda: refaz a ingestão e propaga até a gold.

Para corrigir um intervalo (a API revisou dados, um lote veio errado...). Dispare
pelo botão "Trigger" do Airflow e preencha as UFs e o período. O mesmo que:
    python -m ingestion.clima --uf SP --inicio 2025-03-01 --fim 2025-03-31 --reprocessar
seguido da silver, do Postgres e do dbt.

Diferente da DAG diária, aqui chegar ao limite da API é falha: foi pedido um
período específico, e ele não ficou completo.
"""

from datetime import date, timedelta

import pendulum
from airflow.sdk import Param, dag, task

from orquestracao.alertas import avisar_falha
from orquestracao.containers import tarefa_dbt, tarefa_spark

POOL_API = "api_open_meteo"


@dag(
    dag_id="clima_reprocessar",
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "alisson",
        "retries": 0,
        "execution_timeout": timedelta(hours=4),
        "on_failure_callback": avisar_falha,
    },
    params={
        "ufs": Param(["SP"], type="array", description="UFs a reprocessar, ex.: SP, RJ"),
        "inicio": Param("2025-01-01", type="string", format="date", description="Data inicial"),
        "fim": Param("2025-01-31", type="string", format="date", description="Data final"),
    },
    doc_md=__doc__,
    tags=["clima", "fase-5", "manual"],
)
def clima_reprocessar():
    @task(pool=POOL_API)
    def reprocessar_clima(params: dict | None = None) -> None:
        from ingestion.clima import carregar_clima

        carregar_clima(
            params["ufs"],
            date.fromisoformat(params["inicio"]),
            date.fromisoformat(params["fim"]),
            reprocessar=True,
        )

    silver = tarefa_spark("silver_clima", "silver_clima.py")
    postgres = tarefa_spark("silver_postgres", "silver_postgres.py")
    dbt = tarefa_dbt("dbt_build", "build")

    reprocessar_clima() >> silver >> postgres >> dbt


clima_reprocessar()
