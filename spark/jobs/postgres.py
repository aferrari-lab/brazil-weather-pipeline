"""Acesso ao Postgres (warehouse) pelos jobs Spark: conexão JDBC e o SQL das cargas.

A montagem da conexão e do SQL são funções puras, sem pyspark, para poderem ser
testadas fora do container. Só a execução precisa de uma SparkSession.
"""

import os
from collections.abc import Mapping, Sequence

# Dentro da rede do Docker o Postgres é "postgres:5432". A POSTGRES_PORT do .env
# é a porta exposta no seu PC, e não serve aqui dentro.
HOST_PADRAO = "postgres"
PORTA_INTERNA = 5432


def montar_jdbc(ambiente: Mapping[str, str] | None = None) -> dict[str, str]:
    """Monta as opções de conexão JDBC do warehouse a partir do .env."""
    ambiente = os.environ if ambiente is None else ambiente

    obrigatorias = ("WAREHOUSE_USER", "WAREHOUSE_PASSWORD", "WAREHOUSE_DB")
    faltando = [nome for nome in obrigatorias if not ambiente.get(nome)]
    if faltando:
        raise OSError(f"Variáveis de ambiente do warehouse não definidas: {faltando}")

    host = ambiente.get("WAREHOUSE_HOST", HOST_PADRAO)
    # O reWriteBatchedInserts faz o driver juntar os INSERTs de cada lote em um
    # só comando, o que deixa a gravação várias vezes mais rápida no Postgres.
    url = (
        f"jdbc:postgresql://{host}:{PORTA_INTERNA}/{ambiente['WAREHOUSE_DB']}"
        "?reWriteBatchedInserts=true"
    )
    return {
        "url": url,
        "user": ambiente["WAREHOUSE_USER"],
        "password": ambiente["WAREHOUSE_PASSWORD"],
        "driver": "org.postgresql.Driver",
    }


def sql_trocar_particoes(
    tabela: str, staging: str, colunas: Sequence[str], particoes: Sequence[str]
) -> list[str]:
    """SQL que substitui, na tabela, as partições que estão na staging.

    Apaga do destino as partições presentes na staging, insere as linhas novas e
    descarta a staging. Rodando os três numa transação, quem consulta a tabela
    nunca vê a partição pela metade.
    """
    # Correlaciono pelo alias "t" (destino) e "s" (staging). O EXISTS funciona no
    # Postgres e em outros bancos, o que permite testar este SQL fora do Postgres.
    condicao = " AND ".join(f"s.{coluna} = t.{coluna}" for coluna in particoes)
    lista = ", ".join(colunas)
    return [
        f"DELETE FROM {tabela} AS t WHERE EXISTS (SELECT 1 FROM {staging} AS s WHERE {condicao})",
        f"INSERT INTO {tabela} ({lista}) SELECT {lista} FROM {staging}",
        f"DROP TABLE {staging}",
    ]


# Tipos do Spark (como aparecem em DataFrame.dtypes) e o equivalente no Postgres.
TIPOS_POSTGRES = {
    "string": "TEXT",
    "boolean": "BOOLEAN",
    "int": "INTEGER",
    "bigint": "BIGINT",
    "double": "DOUBLE PRECISION",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
}


def sql_adicionar_colunas(
    tabela: str, colunas_lake: Sequence[tuple[str, str]], colunas_banco: Sequence[str]
) -> list[str]:
    """SQL que cria no Postgres as colunas que existem no lake e ainda não no banco.

    É a "evolução de schema": quando a silver ganha uma coluna nova, a tabela do
    Postgres ganha também, sem precisar apagar e recriar (o que perderia índices e
    quebraria as views do dbt que dependem dela). Colunas são só acrescentadas,
    nunca removidas ou alteradas, para não perder dados por engano.
    """
    existentes = {coluna.lower() for coluna in colunas_banco}
    comandos = []
    for coluna, tipo_spark in colunas_lake:
        if coluna.lower() in existentes:
            continue
        if tipo_spark not in TIPOS_POSTGRES:
            raise ValueError(f"Tipo do Spark sem equivalente no Postgres: {coluna} ({tipo_spark})")
        comandos.append(
            f"ALTER TABLE {tabela} ADD COLUMN IF NOT EXISTS {coluna} {TIPOS_POSTGRES[tipo_spark]}"
        )
    return comandos


def sql_indices_clima(tabela: str) -> list[str]:
    """Índices da tabela de clima. O IF NOT EXISTS deixa seguro rodar sempre."""
    nome = tabela.split(".")[-1]
    return [
        # Chave única: garante, no próprio banco, uma linha por município e dia.
        f"CREATE UNIQUE INDEX IF NOT EXISTS ux_{nome}_chave ON {tabela} (codigo_ibge, data)",
        # Consultas típicas do dashboard: uma UF num período.
        f"CREATE INDEX IF NOT EXISTS ix_{nome}_uf_data ON {tabela} (uf_sigla, data)",
    ]


def executar_sql(spark, jdbc: Mapping[str, str], comandos: Sequence[str]) -> None:
    """Executa comandos SQL no Postgres numa única transação (tudo ou nada)."""
    # O Spark não tem um "execute" para comandos como DELETE ou CREATE INDEX, então
    # abro uma conexão JDBC direto pela JVM do próprio Spark, com o mesmo driver.
    gerenciador = spark.sparkContext._jvm.java.sql.DriverManager
    conexao = gerenciador.getConnection(jdbc["url"], jdbc["user"], jdbc["password"])
    try:
        conexao.setAutoCommit(False)
        comando = conexao.createStatement()
        for sql in comandos:
            comando.execute(sql)
        conexao.commit()
    except Exception:
        # Se qualquer comando falhar, desfaço todos: o banco volta ao estado anterior.
        conexao.rollback()
        raise
    finally:
        conexao.close()


def consultar(spark, jdbc: Mapping[str, str], sql: str):
    """Roda uma consulta no Postgres e devolve a primeira linha (ou None)."""
    resultado = spark.read.format("jdbc").options(**jdbc).option("query", sql).load()
    return resultado.first()


def colunas_da_tabela(spark, jdbc: Mapping[str, str], tabela: str) -> list[str]:
    """Lista as colunas de uma tabela do Postgres (vazia se ela não existir)."""
    esquema, nome = tabela.split(".")
    sql = (
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_schema = '{esquema}' AND table_name = '{nome}'"
    )
    resultado = spark.read.format("jdbc").options(**jdbc).option("query", sql).load()
    return [linha["column_name"] for linha in resultado.collect()]
