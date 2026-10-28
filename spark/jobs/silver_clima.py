"""Camada silver do clima: bronze → tabela limpa, padronizada e com flags de qualidade.

O que este job faz com o bronze:
- padroniza nomes em português e deixa a unidade no nome da coluna (temp_max_c, chuva_mm...);
- deduplica: uma linha por município e dia, ficando com a extração mais recente;
- enriquece com o nome do município e a região, e calcula a amplitude térmica;
- marca a qualidade (dado_completo, temp_suspeita) em vez de apagar linhas.

Modos:
- incremental (padrão): reprocessa só as partições (ano, mês, UF) que receberam dados
  novos no bronze desde a última execução, e não toca nas outras;
- completo: reprocessa e regrava a silver inteira.

Executar (com o ambiente no ar):
    docker compose run --rm spark /opt/spark/bin/spark-submit silver_clima.py
    docker compose run --rm spark /opt/spark/bin/spark-submit silver_clima.py --modo completo
"""

import argparse

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from comum import caminho_existe, caminho_lake, criar_spark

BRONZE_CLIMA = "bronze/clima_diario"
BRONZE_MUNICIPIOS = "bronze/municipios/municipios.parquet"
SILVER_CLIMA = "silver/clima_diario"

PARTICOES = ["ano", "mes", "uf_sigla"]
CHAVE = ["codigo_ibge", "data"]

# De/para dos nomes do bronze (os da API) para os da silver. Deixo a unidade no
# nome da coluna para ninguém precisar adivinhar se é °C, mm ou km/h.
RENOMEAR = {
    "temperature_2m_max": "temp_max_c",
    "temperature_2m_min": "temp_min_c",
    "temperature_2m_mean": "temp_media_c",
    "precipitation_sum": "chuva_mm",
    "relative_humidity_2m_mean": "umidade_media_pct",
    "wind_speed_10m_max": "vento_max_kmh",
}
MEDIDAS = list(RENOMEAR.values())

# Mesma faixa plausível usada na ingestão. Fora dela, a temperatura é marcada como
# suspeita para investigação, mas a linha continua na tabela.
TEMP_MINIMA_C, TEMP_MAXIMA_C = -15.0, 50.0
# Folga para a comparação entre média, mínima e máxima, que vêm arredondadas da API.
TOLERANCIA_C = 0.1

COLUNAS_SILVER = [
    "codigo_ibge",
    "municipio",
    "uf_sigla",
    "regiao",
    "data",
    "ano",
    "mes",
    *MEDIDAS,
    "amplitude_termica_c",
    "dado_completo",
    "temp_suspeita",
    "_extraido_em",
    "_gravado_em",
    "_processado_em",
]


def ler_bronze(spark: SparkSession, caminho_bronze: str) -> DataFrame:
    """Lê o bronze do clima, aceitando arquivos gravados antes e depois de "_gravado_em".

    Os arquivos antigos do bronze não têm a coluna "_gravado_em". O mergeSchema
    junta os schemas de todos os arquivos (a coluna vem nula nos antigos) e, para
    esses, uso a hora da extração no lugar, que era o critério da época.
    """
    bronze = spark.read.option("mergeSchema", "true").parquet(caminho_bronze)
    if "_gravado_em" not in bronze.columns:
        bronze = bronze.withColumn("_gravado_em", F.lit(None).cast("timestamp"))
    return bronze.withColumn("_gravado_em", F.coalesce("_gravado_em", "_extraido_em"))


def deduplicar(clima: DataFrame) -> DataFrame:
    """Mantém uma linha por município e dia: a gravada por último no bronze."""
    # O bronze é append-only: um dia pode aparecer mais de uma vez (um lote refeito,
    # um dia que veio nulo e foi buscado de novo). A janela numera as versões de cada
    # (município, dia) da gravação mais nova para a mais antiga, e fico com a primeira.
    janela = Window.partitionBy(*CHAVE).orderBy(
        F.col("_gravado_em").desc(), F.col("_extraido_em").desc()
    )
    return (
        clima.withColumn("_ordem", F.row_number().over(janela))
        .filter(F.col("_ordem") == 1)
        .drop("_ordem")
    )


def marcar_qualidade(clima: DataFrame) -> DataFrame:
    """Adiciona as flags de qualidade, sem remover nenhuma linha."""
    completo = F.lit(True)
    for medida in MEDIDAS:
        completo = completo & F.col(medida).isNotNull()

    temperaturas = ["temp_max_c", "temp_min_c", "temp_media_c"]
    fora_da_faixa = F.lit(False)
    for coluna in temperaturas:
        fora_da_faixa = fora_da_faixa | ~F.col(coluna).between(TEMP_MINIMA_C, TEMP_MAXIMA_C)

    # Incoerências físicas: a mínima não pode passar da máxima, e a média tem que
    # ficar entre as duas (com uma pequena folga por causa do arredondamento).
    incoerente = (F.col("temp_min_c") > F.col("temp_max_c") + TOLERANCIA_C) | (
        ~F.col("temp_media_c").between(
            F.col("temp_min_c") - TOLERANCIA_C, F.col("temp_max_c") + TOLERANCIA_C
        )
    )

    return clima.withColumn("dado_completo", completo).withColumn(
        # O coalesce trata os nulos: comparação com nulo dá nulo, e nulo aqui
        # significa "não deu para avaliar", então não marco como suspeita.
        "temp_suspeita",
        F.coalesce(fora_da_faixa | incoerente, F.lit(False)),
    )


def transformar(clima_bronze: DataFrame, municipios: DataFrame) -> DataFrame:
    """Transforma o clima do bronze na tabela da silver. Não lê nem grava nada."""
    clima = clima_bronze
    for antigo, novo in RENOMEAR.items():
        clima = clima.withColumnRenamed(antigo, novo)

    clima = deduplicar(clima)

    # O cadastro de municípios é pequeno (5.571 linhas): o broadcast manda uma
    # cópia dele para cada executor e evita embaralhar a tabela grande de clima.
    cadastro = municipios.select("codigo_ibge", F.col("nome").alias("municipio"), "regiao")
    clima = clima.join(F.broadcast(cadastro), on="codigo_ibge", how="left")

    clima = clima.withColumn(
        "amplitude_termica_c", F.round(F.col("temp_max_c") - F.col("temp_min_c"), 1)
    )
    clima = marcar_qualidade(clima)
    clima = clima.withColumn("_processado_em", F.current_timestamp())

    return clima.select(*COLUNAS_SILVER)


def particoes_com_dados_novos(clima_bronze: DataFrame, desde) -> list[tuple]:
    """Lista as partições (ano, mês, UF) do bronze gravadas depois de "desde"."""
    # Uso a hora da gravação, e não a da extração: com cargas em paralelo, um lote
    # extraído antes pode ser gravado depois, e pela extração ele ficaria de fora.
    novas = clima_bronze.filter(F.col("_gravado_em") > F.lit(desde))
    return [tuple(linha) for linha in novas.select(*PARTICOES).distinct().collect()]


def filtrar_particoes(df: DataFrame, particoes: list[tuple]) -> DataFrame:
    """Mantém só as linhas das partições pedidas."""
    condicao = F.lit(False)
    for ano, mes, uf in particoes:
        condicao = condicao | (
            (F.col("ano") == ano) & (F.col("mes") == mes) & (F.col("uf_sigla") == uf)
        )
    return df.filter(condicao)


def validar(silver: DataFrame) -> dict:
    """Confere a silver antes de gravar. Chaves erradas interrompem; o resto vira aviso."""
    resumo = (
        silver.agg(
            F.count("*").alias("linhas"),
            F.countDistinct(*CHAVE).alias("chaves_unicas"),
            F.sum(F.col("codigo_ibge").isNull().cast("int")).alias("sem_codigo"),
            F.sum(F.col("data").isNull().cast("int")).alias("sem_data"),
            F.sum(F.col("municipio").isNull().cast("int")).alias("sem_municipio"),
            F.sum((~F.col("dado_completo")).cast("int")).alias("incompletas"),
            F.sum(F.col("temp_suspeita").cast("int")).alias("suspeitas"),
        )
        .first()
        .asDict()
    )

    # O sum de uma tabela vazia vem nulo; troco por zero para as contas abaixo.
    resumo = {chave: valor or 0 for chave, valor in resumo.items()}

    if resumo["sem_codigo"] or resumo["sem_data"]:
        raise ValueError(f"Linhas sem código IBGE ou sem data na silver: {resumo}")
    if resumo["linhas"] != resumo["chaves_unicas"]:
        raise ValueError(f"A silver tem linhas duplicadas por município e dia: {resumo}")

    linhas = resumo["linhas"] or 1
    print(
        f"Silver: {resumo['linhas']} linhas | "
        f"incompletas: {resumo['incompletas'] / linhas:.2%} | "
        f"temperatura suspeita: {resumo['suspeitas'] / linhas:.2%} | "
        f"sem município no cadastro: {resumo['sem_municipio']}"
    )
    return resumo


def ultima_gravacao_processada(spark: SparkSession, caminho_silver: str):
    """Até que gravação do bronze a silver já foi processada.

    Devolve None quando a silver ainda não existe, ou quando é de uma versão antiga
    (sem "_gravado_em"): nos dois casos, a silver precisa ser gerada por inteiro.
    """
    # Verifico a existência antes de ler, em vez de interpretar a mensagem de erro
    # da leitura: assim uma falha de rede ou de credencial nunca é confundida com
    # "a silver não existe" (o que faria reprocessar tudo por engano).
    if not caminho_existe(spark, caminho_silver):
        return None
    silver = spark.read.parquet(caminho_silver)
    if "_gravado_em" not in silver.columns:
        print("Silver de uma versão anterior (sem _gravado_em): será regerada por inteiro.")
        return None
    return silver.agg(F.max("_gravado_em")).first()[0]


def processar(
    spark: SparkSession,
    caminho_bronze: str,
    caminho_municipios: str,
    caminho_silver: str,
    modo: str = "incremental",
) -> dict:
    """Lê o bronze, transforma, valida e grava a silver. Devolve o resumo da validação."""
    clima_bronze = ler_bronze(spark, caminho_bronze)
    municipios = spark.read.parquet(caminho_municipios)

    if modo == "incremental":
        desde = ultima_gravacao_processada(spark, caminho_silver)
        if desde is None:
            print("Silver ainda não existe (ou é de outra versão): rodando a carga completa.")
            modo = "completo"
        else:
            particoes = particoes_com_dados_novos(clima_bronze, desde)
            if not particoes:
                print(f"Nada novo no bronze desde {desde}. Silver já está em dia.")
                return {"linhas": 0}
            print(f"{len(particoes)} partição(ões) com dados novos desde {desde}.")
            # Reprocesso as partições inteiras (e não só as linhas novas) para a
            # deduplicação enxergar todas as versões de cada município e dia.
            clima_bronze = filtrar_particoes(clima_bronze, particoes)

    silver = transformar(clima_bronze, municipios)
    resumo = validar(silver)

    # "dynamic" regrava só as partições presentes nos dados (incremental); "static"
    # apaga a silver inteira antes de gravar (completo).
    sobrescrita = "dynamic" if modo == "incremental" else "static"
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", sobrescrita)
    silver.write.mode("overwrite").partitionBy(*PARTICOES).parquet(caminho_silver)

    print(f"Silver gravada em {caminho_silver} (modo {modo}).")
    return resumo


def main() -> None:
    parser = argparse.ArgumentParser(description="Gera a camada silver do clima.")
    parser.add_argument("--modo", choices=["incremental", "completo"], default="incremental")
    argumentos = parser.parse_args()

    spark = criar_spark("silver_clima")
    processar(
        spark,
        caminho_lake(BRONZE_CLIMA),
        caminho_lake(BRONZE_MUNICIPIOS),
        caminho_lake(SILVER_CLIMA),
        argumentos.modo,
    )
    spark.stop()


if __name__ == "__main__":
    main()
