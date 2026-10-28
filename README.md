# data_engineering — Clima do Brasil

Pipeline de engenharia de dados de ponta a ponta com o clima diário de todos os municípios brasileiros desde 2016 ([Open-Meteo](https://open-meteo.com/) + IBGE), do data lake ao dashboard e à previsão de temperatura.

```
Fontes (Open-Meteo, IBGE) → Bronze (MinIO, Parquet) → Silver (PySpark) → Gold (dbt + Postgres) → Metabase
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
| Data lake | MinIO (S3 local) |
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
| Airflow | http://localhost:8080 | sem login (modo dev) |
| MinIO console | http://localhost:9001 | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` |
| Postgres | `localhost:5432` | `WAREHOUSE_USER` / `WAREHOUSE_PASSWORD` |

**Verificação:** no Airflow, ative e dispare a DAG `smoke_test`. Ela confirma a conexão com o Postgres e com o MinIO.

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
- **MinIO:** a distribuição comunitária deixou de publicar imagens atualizadas; para estudo local atende bem. As imagens ficam configuráveis no `.env` caso eu migre para outro S3 compatível.
- **Sem login no Airflow** (`SIMPLE_AUTH_MANAGER_ALL_ADMINS`): apenas para desenvolvimento local.
