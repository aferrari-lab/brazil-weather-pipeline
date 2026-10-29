"""Tarefas que rodam os jobs do Spark e do dbt em containers.

O Airflow não tem Spark nem dbt instalados: ele pede ao Docker para subir a imagem
de cada um (as mesmas do "docker compose run"), espera o job terminar e traz o log.
O pedido passa pelo docker-proxy, que só libera o que é preciso para criar e
acompanhar containers (ver docker-compose.yml).

O código dos jobs vai dentro das imagens. Depois de mudar um job, um modelo do dbt
ou a previsão, reconstrua as imagens: docker compose build spark dbt ml
"""

import os
from collections.abc import Mapping

IMAGEM_SPARK = os.getenv("PIPELINE_IMAGEM_SPARK", "brazil-weather-spark:3.5.6")
IMAGEM_DBT = os.getenv("PIPELINE_IMAGEM_DBT", "brazil-weather-dbt:1.10")
IMAGEM_ML = os.getenv("PIPELINE_IMAGEM_ML", "brazil-weather-ml:1.0")
DOCKER_URL = os.getenv("PIPELINE_DOCKER_URL", "tcp://docker-proxy:2375")
# Os containers dos jobs entram na rede do compose para enxergar o lake e o Postgres.
REDE_DOCKER = os.getenv("PIPELINE_REDE_DOCKER", "brazil-weather")

# Credenciais que repasso do Airflow (que as recebe do .env) para os jobs.
VARIAVEIS_REPASSADAS = (
    "LAKE_ACCESS_KEY",
    "LAKE_SECRET_KEY",
    "LAKE_BUCKET",
    "WAREHOUSE_USER",
    "WAREHOUSE_PASSWORD",
    "WAREHOUSE_DB",
)

# Endereços de dentro da rede do Docker (o .env traz os de fora, com localhost).
ENDERECOS_INTERNOS = {
    "S3_ENDPOINT_URL": "http://lake:9000",
    "WAREHOUSE_HOST": "postgres",
}

# Spark e dbt dividem uma fila de uma vaga: um job de cada vez, para não disputar
# a memória do PC e para a silver nunca ser lida enquanto é regravada.
POOL_TRANSFORMACAO = "transformacao"


def ambiente_dos_jobs(ambiente: Mapping[str, str] | None = None) -> dict[str, str]:
    """Monta as variáveis de ambiente dos containers dos jobs."""
    ambiente = os.environ if ambiente is None else ambiente
    repassadas = {nome: ambiente[nome] for nome in VARIAVEIS_REPASSADAS if nome in ambiente}
    return {**repassadas, **ENDERECOS_INTERNOS}


def _opcoes_comuns() -> dict:
    return {
        "docker_url": DOCKER_URL,
        "network_mode": REDE_DOCKER,
        # "private_environment" e não "environment": assim as senhas não aparecem
        # na tela de parâmetros renderizados da tarefa no Airflow.
        "private_environment": ambiente_dos_jobs(),
        # Container que deu certo é apagado; o que falhou fica para investigar.
        "auto_remove": "success",
        # Por padrão o operador monta uma pasta temporária do Airflow no container,
        # o que não funciona quando o Docker roda fora dele (Docker Desktop).
        "mount_tmp_dir": False,
        "pool": POOL_TRANSFORMACAO,
    }


def comando_spark(job: str, *argumentos: str) -> list[str]:
    return ["/opt/spark/bin/spark-submit", "--driver-memory", "2g", job, *argumentos]


def tarefa_spark(task_id: str, job: str, *argumentos: str, **opcoes):
    """Tarefa que roda um job PySpark de spark/jobs/ na imagem do Spark."""
    # Importo aqui dentro para o módulo poder ser testado sem o Airflow instalado.
    from airflow.providers.docker.operators.docker import DockerOperator

    return DockerOperator(
        task_id=task_id,
        image=IMAGEM_SPARK,
        command=comando_spark(job, *argumentos),
        **{**_opcoes_comuns(), **opcoes},
    )


def tarefa_dbt(task_id: str, *argumentos: str, **opcoes):
    """Tarefa que roda um comando do dbt (ex.: "build") na imagem do dbt."""
    from airflow.providers.docker.operators.docker import DockerOperator

    return DockerOperator(
        task_id=task_id,
        image=IMAGEM_DBT,
        command=list(argumentos),
        **{**_opcoes_comuns(), **opcoes},
    )


def tarefa_ml(task_id: str, *argumentos: str, **opcoes):
    """Tarefa que roda o job de previsão (ml/previsao.py) na imagem ml."""
    from airflow.providers.docker.operators.docker import DockerOperator

    return DockerOperator(
        task_id=task_id,
        image=IMAGEM_ML,
        command=list(argumentos),
        **{**_opcoes_comuns(), **opcoes},
    )
