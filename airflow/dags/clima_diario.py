"""Pipeline diário do clima: ingestão → silver (Spark) → Postgres → dbt.

Roda todo dia às 6h (horário de Brasília):
1. ingerir_clima: busca na Open-Meteo os dias que faltam para todo o Brasil, até a
   data da execução menos o atraso da API. Para no limite diário da API sem falhar:
   o resto vem na execução seguinte. É assim que a carga histórica do Brasil
   (semanas de cota) acontece sozinha, uma fatia por dia;
2. silver_clima: o Spark limpa e deduplica só as partições que receberam dado novo;
3. silver_postgres: leva essas partições para o Postgres;
4. dbt_build: atualiza a gold (incluindo a classificação de eventos climáticos) e a
   observabilidade, e roda os testes.

Em paralelo, publicar_log_cargas copia o log das cargas do lake para o Postgres
(ops.log_cargas), para o painel de saúde. Ela roda mesmo se a ingestão falhar.

Sobre catchup: fica desligado de propósito. A ingestão não depende da data da
execução para saber o que buscar (ela olha o que falta no bronze), então uma
execução de hoje já recupera qualquer dia perdido. Rodar as execuções antigas
uma a uma só gastaria cota da API.
"""

from datetime import date, timedelta

import pendulum
from airflow.sdk import dag, task

from orquestracao.alertas import avisar_falha
from orquestracao.containers import tarefa_dbt, tarefa_spark
from orquestracao.observabilidade import tarefa_publicar_log_cargas

# Uma carga por vez na API: duas ao mesmo tempo dividiriam a mesma cota diária.
POOL_API = "api_open_meteo"


@dag(
    dag_id="clima_diario",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=8),
    default_args={
        "owner": "alisson",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(hours=1),
        "on_failure_callback": avisar_falha,
    },
    doc_md=__doc__,
    tags=["clima", "fase-5"],
)
def clima_diario():
    @task(
        pool=POOL_API,
        # A carga espera 60 s entre os lotes; com a cota cheia, leva perto de 1 hora.
        execution_timeout=timedelta(hours=4),
        # Limite da API não chega aqui como erro. Erro de verdade (lake fora do ar,
        # resposta inválida) vale uma nova tentativa, mas sem pressa.
        retries=1,
        retry_delay=timedelta(minutes=30),
    )
    def ingerir_clima(ds: str | None = None) -> str:
        # Importo aqui dentro, e não no topo: o Airflow lê este arquivo a cada
        # poucos segundos, e o pandas só precisa ser carregado quando a tarefa roda.
        from ingestion.clima import carregar_clima_brasil, periodo_padrao

        # A data final vem da data lógica da execução ({{ ds }}), e não do relógio:
        # a mesma execução, refeita depois, pede exatamente o mesmo período.
        _, fim = periodo_padrao(hoje=date.fromisoformat(ds))
        return carregar_clima_brasil(fim)

    silver = tarefa_spark("silver_clima", "silver_clima.py")
    postgres = tarefa_spark("silver_postgres", "silver_postgres.py")
    dbt = tarefa_dbt("dbt_build", "build", retries=1)

    ingestao = ingerir_clima()
    log = tarefa_publicar_log_cargas()

    ingestao >> silver >> postgres
    ingestao >> log
    [postgres, log] >> dbt


clima_diario()
