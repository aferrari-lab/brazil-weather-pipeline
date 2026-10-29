# Imagem do dbt do projeto: Python + dbt com o adaptador do Postgres.
# Uso uma imagem própria (e não instalar no .venv do Windows) porque o dbt ainda
# não suporta oficialmente o Python mais recente, e assim o Airflow (Fase 5) e
# qualquer pessoa que clonar o repositório rodam o dbt no mesmo ambiente.
FROM python:3.12-slim

# Versões fixas: dbt-core e adaptador da mesma série (1.10), para o projeto não
# mudar de comportamento sozinho quando sair uma versão nova.
RUN pip install --no-cache-dir "dbt-core==1.10.15" "dbt-postgres==1.10.2"

# Usuário sem privilégios, como na imagem do Spark.
RUN useradd --create-home --uid 1000 dbt
USER dbt

WORKDIR /usr/app/dbt
ENV DBT_PROFILES_DIR=/usr/app/dbt
# O projeto dbt vai dentro da imagem para o Airflow rodá-lo. Pelo terminal, a
# pasta dbt/ é montada por cima (docker-compose.yml), como no Spark.
COPY --chown=dbt:dbt . /usr/app/dbt
ENTRYPOINT ["dbt"]
CMD ["build"]
