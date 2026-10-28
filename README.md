# data_engineering — Clima do Brasil

Pipeline de engenharia de dados de ponta a ponta com o clima diário de todos os municípios brasileiros desde 2016 ([Open-Meteo](https://open-meteo.com/) + IBGE), do data lake ao dashboard e à previsão de temperatura.

```
Fontes (Open-Meteo, IBGE) → Bronze (RustFS, Parquet) → Silver (PySpark) → Gold (dbt + Postgres) → Metabase
                    └──────────────── orquestrado pelo Airflow ────────────────┘
```

## Status

| Fase | Entregável | Status |
| --- | --- | --- |
| 1. Fundação | Ambiente sobe com um comando | ✅ |
| 2. Ingestão bronze | Dados brutos no lake, carga incremental | ⏳ |
| 3. Silver com PySpark | Tabelas limpas + comparativo de performance | ⏳ |
| 4. Modelagem com dbt | Modelo dimensional testado | ⏳ |
| 5. Orquestração com Airflow | Pipeline diário automático | ⏳ |
| 6. Qualidade e observabilidade | Painel de saúde do pipeline | ⏳ |
| 7. Consumo | Dashboards + previsão de temperatura | ⏳ |
| 8. CI/CD e documentação | Repositório com cara de produção | ⏳ |

## Stack

| Camada | Ferramenta |
| --- | --- |
| Infra | Docker Compose |
| Data lake | RustFS (S3 local) |
| Processamento | PySpark |
| Warehouse | Postgres 16 |
| Transformação | dbt |
| Orquestração | Apache Airflow 3 (LocalExecutor) |
| BI | Metabase |
| Qualidade de código | ruff, pre-commit, pytest |

## Como rodar

Pré-requisitos: Docker Desktop, Python 3.11+ e Git.

```bash
# 1. Variáveis de ambiente
cp .env.example .env        # depois troque as senhas e o AIRFLOW_JWT_SECRET

# 2. Ambiente Python local (lint, testes, notebooks)
python -m venv .venv
.venv\Scripts\activate      # Windows  (Linux/Mac: source .venv/bin/activate)
pip install -r requirements-dev.txt
pre-commit install

# 3. Subir tudo
docker compose up -d
docker compose ps           # aguarde todos ficarem "healthy"
```

| Serviço | Endereço | Acesso |
| --- | --- | --- |
| Airflow | http://localhost:8080 (ou a `AIRFLOW_PORT` do `.env`) | sem login (modo dev) |
| RustFS console | http://localhost:9001 | `LAKE_ACCESS_KEY` / `LAKE_SECRET_KEY` |
| Postgres | `localhost:5432` | `WAREHOUSE_USER` / `WAREHOUSE_PASSWORD` |

**Verificação:** no Airflow, ative e dispare a DAG `smoke_test`. Ela confirma a conexão com o Postgres e com o lake.

Com `make` instalado (Git Bash/WSL): `make setup`, `make up`, `make down`, `make lint`, `make test`, `make help`.

## Estrutura

```
├── airflow/
│   ├── dags/          # DAGs (smoke_test.py já incluída)
│   ├── plugins/
│   ├── config/
│   └── logs/          # gerado em runtime, fora do Git
├── ingestion/         # extratores IBGE e Open-Meteo (Fase 2)
├── spark/             # jobs PySpark bronze → silver (Fase 3)
├── dbt/               # projeto dbt silver → gold (Fase 4)
├── docker/postgres/init/  # cria banco do Airflow e schemas silver/gold/ops
├── notebooks/         # exploração
├── tests/
├── docs/
├── docker-compose.yml
├── Makefile
└── .env.example
```

## Decisões técnicas

- **Um Postgres, dois bancos:** `warehouse` (dados) e `airflow` (metadados), para economizar memória no ambiente local. Em produção seriam instâncias separadas.
- **LocalExecutor:** dispensa Redis e workers Celery; suficiente para um pipeline diário em uma máquina.
- **Conexões por variável de ambiente** (`AIRFLOW_CONN_WAREHOUSE`, `AIRFLOW_CONN_LAKE`): nenhuma credencial no código ou no banco do Airflow.
- **RustFS no lugar do MinIO:** a MinIO parou de publicar imagens da edição comunitária em 2025 e o projeto foi arquivado em 2026. O RustFS é compatível com a API S3, então o código usa `boto3`/`s3fs` normalmente e trocar por AWS S3 depois é só mudar o endpoint.
- **Sem login no Airflow** (`SIMPLE_AUTH_MANAGER_ALL_ADMINS`): apenas para desenvolvimento local.

## O que eu faria em produção

Este projeto roda inteiro na minha máquina, com escolhas pensadas para estudo. Em um ambiente real, eu mudaria:

| Componente | Aqui (local) | Em produção |
| --- | --- | --- |
| Data lake | RustFS (S3 local) | AWS S3, Google Cloud Storage ou Azure Blob. O RustFS ainda é jovem (release candidate) e serve bem para desenvolvimento, mas não para dados críticos. Como o código fala o protocolo S3, a troca é só o endpoint e as credenciais. |
| Banco | Um Postgres com dois bancos (warehouse e metadados do Airflow) | Instâncias separadas e gerenciadas (RDS, Cloud SQL), com backup automático. O warehouse poderia ir para BigQuery, Snowflake ou Redshift, conforme o volume. |
| Airflow | LocalExecutor, sem login | Serviço gerenciado (MWAA, Cloud Composer, Astronomer) ou KubernetesExecutor, com autenticação e controle de acesso por usuário. |
| Segredos | Arquivo `.env` | Secrets Manager, Vault ou o secrets backend do próprio Airflow. |
| Imagens | Tag `latest` em parte dos serviços | Versões fixas e atualizadas de forma controlada. |
| Processamento | PySpark local | Spark gerenciado (EMR, Dataproc, Databricks), dimensionado pelo volume. |

> Seção em evolução: será completada na Fase 8 com custos, monitoramento e segurança.
