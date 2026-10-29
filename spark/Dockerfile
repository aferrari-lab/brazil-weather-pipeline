# Imagem do Spark do projeto: a imagem oficial do Apache Spark + os conectores
# que ela não traz de fábrica (S3 para o lake e JDBC para o Postgres).
#
# Versões casadas: o Spark 3.5.6 vem com o Hadoop 3.3.4, e o conector S3A
# (hadoop-aws) precisa ser exatamente da mesma versão do Hadoop. O
# aws-java-sdk-bundle 1.12.262 é a versão que o hadoop-aws 3.3.4 espera.
FROM apache/spark:3.5.6-scala2.12-java17-python3-ubuntu

ARG HADOOP_AWS_VERSAO=3.3.4
ARG AWS_SDK_VERSAO=1.12.262
ARG POSTGRES_JDBC_VERSAO=42.7.4
ARG MAVEN=https://repo1.maven.org/maven2

USER root

# Baixo os .jar do Maven Central para a pasta de bibliotecas do Spark (com wget,
# que já vem na imagem oficial; o curl não vem).
# Fazer isso na imagem (e não com --packages na hora de rodar) deixa cada job
# mais rápido e funcionando mesmo sem internet depois de construída.
RUN set -eux; \
    cd /opt/spark/jars; \
    wget -q "${MAVEN}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSAO}/hadoop-aws-${HADOOP_AWS_VERSAO}.jar"; \
    wget -q "${MAVEN}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSAO}/aws-java-sdk-bundle-${AWS_SDK_VERSAO}.jar"; \
    wget -q "${MAVEN}/org/postgresql/postgresql/${POSTGRES_JDBC_VERSAO}/postgresql-${POSTGRES_JDBC_VERSAO}.jar"; \
    chmod 644 /opt/spark/jars/*.jar

# Bibliotecas Python que os jobs usam além do pyspark: o benchmark compara o Spark
# com o pandas, e o pyarrow é o que lê e grava Parquet no pandas.
RUN pip install --no-cache-dir "pandas==2.2.3" "pyarrow==17.0.0"

# Volto para o usuário sem privilégios da imagem oficial.
USER spark

WORKDIR /opt/spark/jobs
# Levo os jobs para dentro da imagem: é assim que o Airflow os roda. No uso pelo
# terminal ("docker compose run spark"), a pasta spark/jobs é montada por cima,
# então uma edição vale na hora, sem reconstruir.
COPY --chown=spark:spark jobs/ /opt/spark/jobs/
