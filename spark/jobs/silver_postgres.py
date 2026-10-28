"""Copia a silver do lake para o Postgres (schema silver), de onde o dbt vai ler.

Tabelas no Postgres:
- silver.clima_diario: o clima diário, com chave única (codigo_ibge, data);
- silver.municipios: o cadastro de municípios, regravado inteiro a cada carga.

Modos:
- incremental (padrão): leva só as partições (ano, mês, UF) da silver que foram
  reprocessadas depois da última carga, e as troca no Postgres numa transação;
- completo: regrava a tabela inteira (com TRUNCATE, preservando os índices).

Executar (com o ambiente no ar):
    docker compose run --rm spark /opt/spark/bin/spark-submit silver_postgres.py
    docker compose run --rm spark /opt/spark/bin/spark-submit silver_postgres.py --modo completo
"""

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from comum import caminho_lake, criar_spark
from postgres import (
    colunas_da_tabela,
    consultar,
    executar_sql,
    montar_jdbc,
    sql_adicionar_colunas,
    sql_indices_clima,
    sql_trocar_particoes,
)
from silver_clima import BRONZE_MUNICIPIOS, PARTICOES, SILVER_CLIMA, filtrar_particoes

TABELA_CLIMA = "silver.clima_diario"
TABELA_STAGING = "silver.clima_diario_stg"
TABELA_MUNICIPIOS = "silver.municipios"

# Quantas linhas o driver manda ao Postgres por vez, e quantas conexões gravam em
# paralelo. Valores moderados para não sobrecarregar o Postgres local.
TAMANHO_LOTE_JDBC = 10_000
CONEXOES_PARALELAS = 4


def gravar_tabela(df: DataFrame, jdbc: dict, tabela: str, truncar: bool) -> None:
    """Grava um DataFrame numa tabela do Postgres, substituindo o conteúdo."""
    (
        df.repartition(CONEXOES_PARALELAS)
        .write.format("jdbc")
        .options(**jdbc)
        .option("dbtable", tabela)
        .option("batchsize", TAMANHO_LOTE_JDBC)
        # Com truncate, o Spark esvazia a tabela em vez de apagá-la e recriá-la:
        # assim os índices e permissões que criei continuam lá.
        .option("truncate", str(truncar).lower())
        .mode("overwrite")
        .save()
    )


def evoluir_schema(spark: SparkSession, jdbc: dict, tabela: str, df: DataFrame) -> None:
    """Acrescenta na tabela do Postgres as colunas novas que a silver ganhou."""
    comandos = sql_adicionar_colunas(tabela, df.dtypes, colunas_da_tabela(spark, jdbc, tabela))
    if comandos:
        print(f"{tabela}: adicionando {len(comandos)} coluna(s) nova(s).")
        executar_sql(spark, jdbc, comandos)


def tabela_existe(spark: SparkSession, jdbc: dict, tabela: str) -> bool:
    """Diz se a tabela já existe no Postgres."""
    linha = consultar(spark, jdbc, f"SELECT to_regclass('{tabela}') IS NOT NULL AS existe")
    return bool(linha["existe"])


def ultima_carga(spark: SparkSession, jdbc: dict):
    """Até que processamento da silver o Postgres já recebeu."""
    linha = consultar(spark, jdbc, f"SELECT max(_processado_em) AS ultima FROM {TABELA_CLIMA}")
    return linha["ultima"]


def particoes_reprocessadas(silver: DataFrame, desde) -> list[tuple]:
    """Partições da silver processadas depois de "desde"."""
    novas = silver.filter(F.col("_processado_em") > F.lit(desde))
    return [tuple(linha) for linha in novas.select(*PARTICOES).distinct().collect()]


def carregar_municipios(spark: SparkSession, jdbc: dict, caminho_municipios: str) -> int:
    """Regrava o cadastro de municípios no Postgres (é pequeno: sempre inteiro)."""
    municipios = spark.read.parquet(caminho_municipios).select(
        "codigo_ibge",
        F.col("nome").alias("municipio"),
        "uf_sigla",
        "regiao",
        "latitude",
        "longitude",
        "_extraido_em",
    )
    existe = tabela_existe(spark, jdbc, TABELA_MUNICIPIOS)
    if existe:
        evoluir_schema(spark, jdbc, TABELA_MUNICIPIOS, municipios)
    gravar_tabela(municipios, jdbc, TABELA_MUNICIPIOS, truncar=existe)
    if not existe:
        executar_sql(
            spark,
            jdbc,
            [
                f"CREATE UNIQUE INDEX IF NOT EXISTS ux_municipios_codigo "
                f"ON {TABELA_MUNICIPIOS} (codigo_ibge)"
            ],
        )
    return municipios.count()


def carregar_clima(spark: SparkSession, jdbc: dict, caminho_silver: str, modo: str) -> int:
    """Leva o clima da silver para o Postgres. Devolve as linhas gravadas."""
    silver = spark.read.parquet(caminho_silver)

    if modo == "incremental" and not tabela_existe(spark, jdbc, TABELA_CLIMA):
        print(f"{TABELA_CLIMA} ainda não existe: rodando a carga completa.")
        modo = "completo"

    # Antes de gravar, alinho o schema do Postgres com o da silver: o TRUNCATE (modo
    # completo) e o INSERT da staging (incremental) exigem as mesmas colunas.
    if tabela_existe(spark, jdbc, TABELA_CLIMA):
        evoluir_schema(spark, jdbc, TABELA_CLIMA, silver)

    if modo == "completo":
        existe = tabela_existe(spark, jdbc, TABELA_CLIMA)
        gravar_tabela(silver, jdbc, TABELA_CLIMA, truncar=existe)
        executar_sql(spark, jdbc, sql_indices_clima(TABELA_CLIMA))
        return silver.count()

    desde = ultima_carga(spark, jdbc)
    particoes = particoes_reprocessadas(silver, desde) if desde else None
    if particoes == []:
        print(f"Nada novo na silver desde {desde}. Postgres já está em dia.")
        return 0

    # Partições inteiras (e não só as linhas novas): a silver regrava a partição
    # toda a cada reprocessamento, então a troca também é por partição.
    # Sem "desde", a tabela existe mas está vazia: levo a silver inteira.
    alteradas = silver if particoes is None else filtrar_particoes(silver, particoes)
    quantas = "todas as" if particoes is None else str(len(particoes))
    print(f"Levando {quantas} partição(ões) para o Postgres via staging.")

    # 1) Gravo as partições numa tabela de staging; 2) numa transação, apago essas
    # partições do destino, insiro as novas e descarto a staging. Se o passo 2
    # falhar, o rollback deixa o destino exatamente como estava.
    gravar_tabela(alteradas, jdbc, TABELA_STAGING, truncar=False)
    executar_sql(
        spark,
        jdbc,
        sql_trocar_particoes(TABELA_CLIMA, TABELA_STAGING, silver.columns, PARTICOES),
    )
    return alteradas.count()


def validar_contagens(spark: SparkSession, jdbc: dict, caminho_silver: str) -> None:
    """Confere se o Postgres ficou com o mesmo número de linhas da silver."""
    no_lake = spark.read.parquet(caminho_silver).count()
    no_postgres = consultar(spark, jdbc, f"SELECT count(*) AS linhas FROM {TABELA_CLIMA}")["linhas"]
    if no_lake != no_postgres:
        raise ValueError(f"Silver tem {no_lake} linhas e o Postgres tem {no_postgres}.")
    print(f"Contagens conferem: {no_postgres} linhas no lake e no Postgres.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Copia a silver do lake para o Postgres.")
    parser.add_argument("--modo", choices=["incremental", "completo"], default="incremental")
    argumentos = parser.parse_args()

    spark = criar_spark("silver_postgres")
    jdbc = montar_jdbc()

    municipios = carregar_municipios(spark, jdbc, caminho_lake(BRONZE_MUNICIPIOS))
    print(f"{TABELA_MUNICIPIOS}: {municipios} municípios.")

    linhas = carregar_clima(spark, jdbc, caminho_lake(SILVER_CLIMA), argumentos.modo)
    print(f"{TABELA_CLIMA}: {linhas} linha(s) gravada(s).")

    validar_contagens(spark, jdbc, caminho_lake(SILVER_CLIMA))
    spark.stop()


if __name__ == "__main__":
    main()
