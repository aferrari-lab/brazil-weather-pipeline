"""Testes do SQL e da conexão do módulo postgres.py dos jobs Spark.

O SQL da troca de partições é testado de verdade num banco SQLite em memória:
ele usa só SQL padrão (DELETE com EXISTS, INSERT ... SELECT), que funciona igual
no Postgres. Assim a lógica é conferida sem precisar do Postgres no ar.
"""

import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "spark" / "jobs"))

import postgres  # noqa: E402

AMBIENTE = {"WAREHOUSE_USER": "dw", "WAREHOUSE_PASSWORD": "segredo", "WAREHOUSE_DB": "warehouse"}
COLUNAS = ["codigo_ibge", "data", "ano", "mes", "uf_sigla", "temp_max_c"]
PARTICOES = ["ano", "mes", "uf_sigla"]


# --- Conexão ---
def test_jdbc_usa_o_host_e_a_porta_internos_do_docker():
    jdbc = postgres.montar_jdbc({**AMBIENTE, "POSTGRES_PORT": "5433"})

    # A porta do .env é a do PC; dentro do Docker o Postgres é sempre postgres:5432.
    assert jdbc["url"].startswith("jdbc:postgresql://postgres:5432/warehouse")
    assert "reWriteBatchedInserts=true" in jdbc["url"]
    assert jdbc["driver"] == "org.postgresql.Driver"


def test_jdbc_falha_sem_senha():
    with pytest.raises(OSError, match="WAREHOUSE_PASSWORD"):
        postgres.montar_jdbc({"WAREHOUSE_USER": "dw", "WAREHOUSE_DB": "warehouse"})


# --- Troca de partições, executada num banco de verdade (SQLite) ---
@pytest.fixture
def banco():
    conexao = sqlite3.connect(":memory:")
    # No SQLite, um "schema" é um banco anexado: assim "silver.tabela" funciona.
    conexao.execute("ATTACH DATABASE ':memory:' AS silver")
    definicao = "(codigo_ibge INT, data TEXT, ano INT, mes INT, uf_sigla TEXT, temp_max_c REAL)"
    conexao.execute(f"CREATE TABLE silver.clima {definicao}")
    conexao.execute(f"CREATE TABLE silver.clima_stg {definicao}")
    yield conexao
    conexao.close()


def _inserir(conexao, tabela, linhas):
    conexao.executemany(f"INSERT INTO {tabela} VALUES (?, ?, ?, ?, ?, ?)", linhas)


def _trocar(conexao):
    for sql in postgres.sql_trocar_particoes(
        "silver.clima", "silver.clima_stg", COLUNAS, PARTICOES
    ):
        conexao.execute(sql)


def test_troca_substitui_so_as_particoes_da_staging(banco):
    _inserir(
        banco,
        "silver.clima",
        [
            (1, "2025-01-01", 2025, 1, "SP", 30.0),
            (1, "2025-01-02", 2025, 1, "SP", 31.0),
            (2, "2025-01-01", 2025, 1, "PR", 20.0),  # outra UF: não pode ser tocada
            (1, "2024-12-31", 2024, 12, "SP", 29.0),  # outro mês: não pode ser tocado
        ],
    )
    # A partição 2025/01/SP chega reprocessada, com um dia a mais e um valor corrigido.
    _inserir(
        banco,
        "silver.clima_stg",
        [
            (1, "2025-01-01", 2025, 1, "SP", 30.5),
            (1, "2025-01-02", 2025, 1, "SP", 31.0),
            (1, "2025-01-03", 2025, 1, "SP", 32.0),
        ],
    )

    _trocar(banco)

    linhas = banco.execute(
        "SELECT uf_sigla, data, temp_max_c FROM silver.clima ORDER BY 1, 2"
    ).fetchall()
    assert linhas == [
        ("PR", "2025-01-01", 20.0),
        ("SP", "2024-12-31", 29.0),
        ("SP", "2025-01-01", 30.5),
        ("SP", "2025-01-02", 31.0),
        ("SP", "2025-01-03", 32.0),
    ]


def test_troca_descarta_a_staging(banco):
    _inserir(banco, "silver.clima_stg", [(1, "2025-01-01", 2025, 1, "SP", 30.0)])

    _trocar(banco)

    tabelas = banco.execute("SELECT name FROM silver.sqlite_master WHERE type = 'table'").fetchall()
    assert ("clima_stg",) not in tabelas


def test_troca_sem_linhas_repetidas_ao_rodar_duas_vezes(banco):
    for _ in range(2):
        banco.execute(
            "CREATE TABLE IF NOT EXISTS silver.clima_stg (codigo_ibge INT, data TEXT, "
            "ano INT, mes INT, uf_sigla TEXT, temp_max_c REAL)"
        )
        _inserir(banco, "silver.clima_stg", [(1, "2025-01-01", 2025, 1, "SP", 30.0)])
        _trocar(banco)

    assert banco.execute("SELECT count(*) FROM silver.clima").fetchone() == (1,)


def test_indices_garantem_chave_unica():
    sql = postgres.sql_indices_clima("silver.clima_diario")

    assert sql[0] == (
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_clima_diario_chave "
        "ON silver.clima_diario (codigo_ibge, data)"
    )
    assert all("IF NOT EXISTS" in comando for comando in sql)


# --- Evolução de schema ---
def test_adicionar_colunas_cria_so_as_que_faltam():
    colunas_lake = [
        ("codigo_ibge", "bigint"),
        ("temp_max_c", "double"),
        ("_gravado_em", "timestamp"),
    ]

    sql = postgres.sql_adicionar_colunas(
        "silver.clima", colunas_lake, ["codigo_ibge", "TEMP_MAX_C"]
    )

    assert sql == [
        "ALTER TABLE silver.clima ADD COLUMN IF NOT EXISTS _gravado_em TIMESTAMP",
    ]


def test_adicionar_colunas_sem_diferenca_nao_gera_nada():
    assert postgres.sql_adicionar_colunas("silver.clima", [("a", "int")], ["a"]) == []


def test_adicionar_colunas_recusa_tipo_desconhecido():
    with pytest.raises(ValueError, match="sem equivalente"):
        postgres.sql_adicionar_colunas("silver.clima", [("x", "map<string,int>")], [])
