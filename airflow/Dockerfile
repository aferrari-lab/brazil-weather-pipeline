# Imagem do Airflow do projeto: a oficial + as bibliotecas que a ingestão usa.
#
# A ingestão (Python puro) roda dentro do próprio Airflow. Spark e dbt NÃO são
# instalados aqui: cada um roda na sua imagem, e o Airflow só pede ao Docker
# para subir o container certo (DockerOperator).
FROM apache/airflow:3.1.0

ARG AIRFLOW_VERSION=3.1.0
ARG PYTHON_VERSION=3.12

COPY requirements.txt /tmp/requirements-airflow.txt

# Instalo junto com o próprio Airflow e contra o arquivo de constraints oficial da
# versão: assim o pip não troca o Airflow de versão nem pega uma biblioteca
# incompatível com ele. É o jeito recomendado pela documentação da imagem.
RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    -r /tmp/requirements-airflow.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
