# Atalhos do projeto. No Windows, rode no Git Bash ou WSL (ou use os comandos docker compose direto).
.PHONY: help setup up down restart logs ps clean lint format test

help:            ## Lista os comandos
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-10s %s\n", $$1, $$2}'

setup:           ## Cria .env, venv e instala dependências + pre-commit
	@test -f .env || cp .env.example .env
	python -m venv .venv
	.venv/bin/pip install -r requirements-dev.txt || .venv/Scripts/pip install -r requirements-dev.txt
	.venv/bin/pre-commit install || .venv/Scripts/pre-commit install

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
