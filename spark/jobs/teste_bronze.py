"""Job de teste da Fase 3: confere se o Spark lê o bronze do lake.

Executar (com o ambiente no ar):
    docker compose run --rm spark /opt/spark/bin/spark-submit teste_bronze.py
    (ou só: docker compose run --rm spark, que roda este job por padrão)
"""

from pyspark.sql import functions as F

from comum import caminho_lake, criar_spark


def main() -> None:
    spark = criar_spark("teste_bronze")

    # O Spark entende sozinho as pastas "ano=.../mes=.../uf_sigla=..." do bronze
    # e as transforma em colunas (é o "partition discovery").
    clima = spark.read.parquet(caminho_lake("bronze/clima_diario"))
    municipios = spark.read.parquet(caminho_lake("bronze/municipios/municipios.parquet"))

    print("\n=== Schema do clima no bronze ===")
    clima.printSchema()

    print("=== Linhas, municípios e período por UF ===")
    (
        clima.groupBy("uf_sigla")
        .agg(
            F.count("*").alias("linhas"),
            F.countDistinct("codigo_ibge").alias("municipios"),
            F.min("data").alias("primeiro_dia"),
            F.max("data").alias("ultimo_dia"),
        )
        .orderBy("uf_sigla")
        .show(truncate=False)
    )

    print(f"Municípios no cadastro: {municipios.count()}")
    print(f"Partições de clima lidas: {clima.rdd.getNumPartitions()}")

    spark.stop()


if __name__ == "__main__":
    main()
