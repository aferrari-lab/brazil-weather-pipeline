# Atalhos do projeto. No Windows, rode no Git Bash ou WSL (ou use os comandos docker compose direto).
.PHONY: help setup up down restart logs ps clean lint format test spark-build spark-teste spark-shell spark-silver spark-silver-completo spark-postgres spark-benchmark dbt-build dbt-test dbt-freshness dbt-docs build airflow-reprocessar

help:            ## Lista os comandos
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-10s %s\n", $$1, $$2}'

setup:           ## Cria .env, venv e instala dependências + pre-commit
	@test -f .env || cp .env.example .env
	python -m venv .venv
	.venv/bin/pip install -r requirements-dev.txt || .venv/Scripts/pip install -r requirements-dev.txt
	.venv/bin/pre-commit install || .venv/Scripts/pre-commit install

build:           ## Constrói as imagens (Airflow, Spark, dbt). Rode de novo após mudar jobs ou modelos
	docker compose build
	docker compose build spark dbt

up:              ## Sobe todo o ambiente
	docker compose up -d

down:            ## Derruba o ambiente (mantém os dados)
	docker compose down

restart: down up ## Reinicia o ambiente

logs:            ## Acompanha os logs
	docker compose logs -f --tail=100

ps:              ## Status dos serviços
	docker compose ps

clean:           ## Derruba e APAGA volumes (Postgres e MinIO)
	docker compose down -v

lint:            ## Checa o código Python
	ruff check .

format:          ## Formata o código Python
	ruff format . && ruff check . --fix

test:            ## Roda os testes
	pytest -q

spark-build:     ## Constrói a imagem do Spark (com os conectores do S3 e do Postgres)
	docker compose build spark

spark-teste:     ## Roda o job de teste: o Spark lendo o bronze do lake
	docker compose run --rm spark

spark-shell:     ## Abre o PySpark interativo já conectado ao lake
	docker compose run --rm spark /opt/spark/bin/pyspark

spark-silver:    ## Atualiza a silver do clima (só as partições com dados novos)
	docker compose run --rm spark /opt/spark/bin/spark-submit --driver-memory 2g silver_clima.py

spark-silver-completo: ## Regera a silver do clima inteira
	docker compose run --rm spark /opt/spark/bin/spark-submit --driver-memory 2g silver_clima.py --modo completo

spark-postgres:  ## Leva a silver do lake para o Postgres (só o que mudou)
	docker compose run --rm spark /opt/spark/bin/spark-submit --driver-memory 2g silver_postgres.py

spark-benchmark: ## Comparativo pandas × PySpark (resultados em docs/benchmark/)
	docker compose run --rm spark /opt/spark/bin/spark-submit --driver-memory 4g benchmark.py

dbt-build:       ## dbt: cria os modelos e roda os testes
	docker compose run --rm dbt build

dbt-test:        ## dbt: só os testes de dados
	docker compose run --rm dbt test

dbt-freshness:   ## dbt: confere se a silver está sendo atualizada
	docker compose run --rm dbt source freshness

dbt-docs:        ## dbt: gera e abre a documentação em http://localhost:8085
	docker compose run --rm dbt docs generate
	docker compose run --rm --service-ports dbt docs serve --host 0.0.0.0 --port 8080

airflow-reprocessar: ## Dispara a DAG de reprocessamento. Ex.: make airflow-reprocessar UF=SP INICIO=2025-03-01 FIM=2025-03-31
	docker compose exec airflow-scheduler airflow dags trigger clima_reprocessar \
		--conf '{"ufs": ["$(UF)"], "inicio": "$(INICIO)", "fim": "$(FIM)"}'
