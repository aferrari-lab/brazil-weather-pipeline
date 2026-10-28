# brazil-weather-pipeline

Pipeline de engenharia de dados de ponta a ponta com o clima diário de todos os municípios brasileiros desde 2025 ([Open-Meteo](https://open-meteo.com/) + IBGE), do data lake ao dashboard e à previsão de temperatura.

```
Fontes (Open-Meteo, IBGE) → Bronze (RustFS, Parquet) → Silver (PySpark) → Gold (dbt + Postgres) → Metabase
                    └──────────────── orquestrado pelo Airflow ────────────────┘
```

## Status

| Fase | Entregável | Status |
| --- | --- | --- |
| 1. Fundação | Ambiente sobe com um comando | ✅ |
| 2. Ingestão bronze | Dados brutos no lake, carga incremental | ✅ código pronto (carga dos dados em andamento) |
| 3. Silver com PySpark | Tabelas limpas + comparativo de performance | 🟡 em andamento |
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
| Processamento | PySpark 3.5.6 (em container Docker) |
| Warehouse | Postgres 16 |
| Transformação | dbt |
| Orquestração | Apache Airflow 3 (LocalExecutor) |
| BI | Metabase |
| Qualidade de código | ruff, pre-commit, pytest |

## Como rodar

Pré-requisitos: Docker Desktop, Python 3.11+ e Git.

```bash
# 1. Variáveis de ambiente
copy .env.example .env      # Windows (Linux/Mac: cp .env.example .env); depois troque as senhas e o AIRFLOW_JWT_SECRET

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

## Ingestão

Com o ambiente no ar (`docker compose up -d`) e o `.venv` ativo:

```bash
python -m ingestion.municipios   # tabela de municípios → s3://lake/bronze/municipios/
python -m ingestion.clima --uf SP --limite 3   # teste: clima de 3 cidades de SP
python -m ingestion.clima --uf SP              # clima de SP: busca só o que falta (incremental)
python -m ingestion.clima --uf SP --reprocessar --inicio 2025-03-01 --fim 2025-03-31  # refaz um período
pytest -q                        # testes, sem precisar de internet nem do lake
```

## Spark

O Spark roda num container Docker, sob demanda (não sobe com `docker compose up`). A imagem é a oficial do Apache Spark com os conectores do S3 (para o lake) e do Postgres.

```bash
docker compose build spark                                   # 1ª vez: constrói a imagem
docker compose run --rm spark                                # job de teste: lê o bronze do lake
docker compose run --rm spark /opt/spark/bin/spark-submit teste_bronze.py   # o mesmo, explícito
docker compose run --rm spark /opt/spark/bin/pyspark         # PySpark interativo
```

Os jobs ficam em `spark/jobs/` e são editados normalmente no VS Code: a pasta é montada dentro do container.

## Estrutura

```
├── airflow/
│   ├── dags/          # DAGs (smoke_test.py já incluída)
│   ├── plugins/
│   ├── config/
│   └── logs/          # gerado em runtime, fora do Git
├── ingestion/         # extratores (Fase 2): lake.py (acesso ao lake), municipios.py (IBGE + coordenadas), clima.py (Open-Meteo)
├── spark/             # Dockerfile do Spark e jobs PySpark em spark/jobs/ (Fase 3)
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
- **Carga incremental com checkpoint:** o clima é buscado só a partir do último dia válido de cada município (`controle/checkpoint_clima.parquet`). A gravação mescla com o que já existe pela chave município + dia, então a carga é incremental e idempotente, e retoma de onde parou se o limite da API for atingido.
- **Log de execuções:** cada carga grava uma linha em `controle/log_cargas/` (início, fim, linhas novas, chamadas à API e status: `sucesso`, `limite_api` ou `erro`), mesmo quando falha. Cada execução grava seu próprio arquivo, então cargas simultâneas nunca sobrescrevem o log uma da outra.
- **Spark em Docker:** rodar o Spark direto no Windows exige Java, `winutils` do Hadoop e variáveis de ambiente. No container, funciona igual em qualquer máquina e já fica na mesma rede do lake e do Postgres. Os conectores (hadoop-aws, AWS SDK, JDBC do Postgres) são baixados na imagem, com versões casadas com o Hadoop 3.3.4 do Spark 3.5.6.
- **Sem login no Airflow** (`SIMPLE_AUTH_MANAGER_ALL_ADMINS`): apenas para desenvolvimento local.

## Fontes e licenças

- **Clima:** dados meteorológicos de [Open-Meteo.com](https://open-meteo.com/), sob licença [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).
- **Municípios:** [IBGE — Instituto Brasileiro de Geografia e Estatística](https://www.ibge.gov.br/), dados públicos.
- **Código:** licença MIT (veja [LICENSE](LICENSE)).

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
