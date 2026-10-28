"""DAG de verificação do ambiente (Fase 1).

Confirma que o Airflow alcança o warehouse (Postgres) e o lake (RustFS).
Ative pela interface em http://localhost:8080 e dispare manualmente.
"""

from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="smoke_test",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["fase-1", "infra"],
)
def smoke_test():
    @task
    def checa_warehouse() -> str:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        versao = PostgresHook(postgres_conn_id="warehouse").get_first("select version()")[0]
        print(f"Postgres OK: {versao}")
        return versao

    @task
    def checa_lake() -> list[str]:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        resposta = S3Hook(aws_conn_id="lake").get_conn().list_buckets()
        buckets = [b["Name"] for b in resposta["Buckets"]]
        print(f"Lake OK, buckets: {buckets}")
        return buckets

    checa_warehouse() >> checa_lake()


smoke_test()
