"""Peças compartilhadas pelos jobs Spark: a sessão ligada ao lake e os caminhos.

Todos os jobs criam a SparkSession por aqui, então a forma de falar com o
RustFS (endpoint, credenciais, estilo de URL) fica definida em um lugar só.
"""

import os
from collections.abc import Mapping

# Endpoint do lake dentro da rede do Docker. O docker-compose também o define
# na variável S3_ENDPOINT_URL do serviço do Spark.
ENDPOINT_PADRAO = "http://lake:9000"


def caminho_lake(caminho: str, ambiente: Mapping[str, str] | None = None) -> str:
    """Monta o endereço S3A de um caminho do lake, ex.: s3a://lake/bronze/clima_diario."""
    ambiente = os.environ if ambiente is None else ambiente
    bucket = ambiente.get("LAKE_BUCKET", "lake")
    # O Spark usa o prefixo "s3a://" (o conector S3 do Hadoop), e não "s3://".
    return f"s3a://{bucket}/{caminho.strip('/')}"


def montar_config_s3a(ambiente: Mapping[str, str] | None = None) -> dict[str, str]:
    """Monta as configurações do conector S3A a partir das variáveis de ambiente.

    Função pura (não importa o pyspark), para poder ser testada fora do container.
    """
    ambiente = os.environ if ambiente is None else ambiente

    # Falho cedo, com mensagem clara, se faltar credencial: sem isso, o erro só
    # apareceria lá dentro do Java, bem mais difícil de entender.
    faltando = [nome for nome in ("LAKE_ACCESS_KEY", "LAKE_SECRET_KEY") if not ambiente.get(nome)]
    if faltando:
        raise OSError(f"Variáveis de ambiente do lake não definidas: {faltando}")

    endpoint = ambiente.get("S3_ENDPOINT_URL", ENDPOINT_PADRAO)
    return {
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.endpoint": endpoint,
        "spark.hadoop.fs.s3a.access.key": ambiente["LAKE_ACCESS_KEY"],
        "spark.hadoop.fs.s3a.secret.key": ambiente["LAKE_SECRET_KEY"],
        # Mesmo motivo do lake.py: servidores S3 locais usam URLs no estilo
        # http://servidor/bucket/arquivo ("path style").
        "spark.hadoop.fs.s3a.path.style.access": "true",
        # O RustFS local responde em http, sem certificado.
        "spark.hadoop.fs.s3a.connection.ssl.enabled": str(endpoint.startswith("https")).lower(),
        # Credenciais simples (chave e segredo), sem buscar perfis da AWS.
        "spark.hadoop.fs.s3a.aws.credentials.provider": (
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        ),
        # Datas e horários sempre em UTC, igual ao que a ingestão grava.
        "spark.sql.session.timeZone": "UTC",
    }


def criar_spark(nome_app: str):
    """Cria a SparkSession do job já conectada ao lake."""
    # Importo o pyspark só aqui dentro: assim o resto do módulo pode ser
    # importado (e testado) em máquinas que não têm o Spark instalado.
    from pyspark.sql import SparkSession

    construtor = SparkSession.builder.appName(nome_app)
    for chave, valor in montar_config_s3a().items():
        construtor = construtor.config(chave, valor)

    spark = construtor.getOrCreate()
    # O Spark é muito verboso no nível INFO; deixo só avisos e erros.
    spark.sparkContext.setLogLevel("WARN")
    return spark
