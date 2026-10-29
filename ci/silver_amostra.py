"""Cria uma silver de amostra no Postgres, para o CI rodar o dbt e a previsão.

No GitHub Actions não há lake nem API: o CI sobe um Postgres vazio, e este script
grava nele as duas tabelas que o Spark grava de verdade (silver.clima_diario e
silver.municipios), com o mesmo schema e dados sintéticos para seis capitais.

A temperatura imita a real: um ciclo anual mais um "tempo" que persiste por alguns
dias. A semente é fixa, então o CI vê sempre os mesmos dados.

Executar:
    python ci/silver_amostra.py   (lê WAREHOUSE_HOST, WAREHOUSE_PORT, WAREHOUSE_DB,
                                   WAREHOUSE_USER e WAREHOUSE_PASSWORD)
"""

import os
from datetime import UTC, date, datetime, timedelta

import numpy as np
import psycopg

INICIO = date(2025, 1, 1)
DIAS = 420
SEMENTE = 7

# (código IBGE, nome, UF, região, latitude, longitude, temperatura média anual)
MUNICIPIOS = [
    (3550308, "São Paulo", "SP", "Sudeste", -23.55, -46.63, 20.5),
    (3509502, "Campinas", "SP", "Sudeste", -22.91, -47.06, 21.5),
    (4106902, "Curitiba", "PR", "Sul", -25.43, -49.27, 17.5),
    (4314902, "Porto Alegre", "RS", "Sul", -30.03, -51.23, 19.5),
    (1302603, "Manaus", "AM", "Norte", -3.12, -60.02, 27.5),
    (2611606, "Recife", "PE", "Nordeste", -8.05, -34.88, 26.0),
]

SQL_TABELAS = """
CREATE SCHEMA IF NOT EXISTS silver;
DROP TABLE IF EXISTS silver.clima_diario;
DROP TABLE IF EXISTS silver.municipios;
CREATE TABLE silver.municipios (
    codigo_ibge bigint, municipio text, uf_sigla text, regiao text,
    latitude double precision, longitude double precision,
    _extraido_em timestamp
);
CREATE TABLE silver.clima_diario (
    codigo_ibge bigint, municipio text, uf_sigla text, regiao text, data date,
    ano integer, mes integer,
    temp_max_c double precision, temp_min_c double precision,
    temp_media_c double precision, chuva_mm double precision,
    umidade_media_pct double precision, vento_max_kmh double precision,
    amplitude_termica_c double precision,
    dado_completo boolean, temp_suspeita boolean,
    _extraido_em timestamp, _gravado_em timestamp, _processado_em timestamp
);
"""


def gerar_clima(agora: datetime) -> list[tuple]:
    gerador = np.random.default_rng(SEMENTE)
    linhas = []
    for codigo, nome, uf, regiao, latitude, _, media_anual in MUNICIPIOS:
        # Quanto mais ao sul, maior a diferença entre verão e inverno.
        amplitude_anual = 1.0 + abs(latitude) / 6
        tempo = 0.0
        for indice in range(DIAS):
            dia = INICIO + timedelta(days=indice)
            ciclo = media_anual + amplitude_anual * np.cos(
                2 * np.pi * (dia.timetuple().tm_yday - 15) / 365.25
            )
            tempo = 0.7 * tempo + gerador.normal(0, 1.5)
            media = round(float(ciclo + tempo), 1)
            amplitude = round(float(8 + gerador.normal(0, 1.5)), 1)
            chuva = round(float(max(0.0, gerador.normal(-2, 6))), 1)
            linhas.append(
                (
                    codigo,
                    nome,
                    uf,
                    regiao,
                    dia,
                    dia.year,
                    dia.month,
                    round(media + amplitude / 2, 1),
                    round(media - amplitude / 2, 1),
                    media,
                    chuva,
                    round(float(np.clip(70 + gerador.normal(0, 10), 20, 100)), 1),
                    round(float(abs(15 + gerador.normal(0, 5))), 1),
                    amplitude,
                    True,
                    False,
                    agora,
                    agora,
                    agora,
                )
            )
    return linhas


def main() -> None:
    agora = datetime.now(UTC).replace(tzinfo=None)
    conexao = psycopg.connect(
        host=os.getenv("WAREHOUSE_HOST", "localhost"),
        port=int(os.getenv("WAREHOUSE_PORT", "5432")),
        dbname=os.environ["WAREHOUSE_DB"],
        user=os.environ["WAREHOUSE_USER"],
        password=os.environ["WAREHOUSE_PASSWORD"],
    )
    with conexao as conn, conn.cursor() as cur:
        cur.execute(SQL_TABELAS)
        cur.executemany(
            "INSERT INTO silver.municipios VALUES (%s, %s, %s, %s, %s, %s, %s)",
            [(c, n, uf, r, lat, lon, agora) for c, n, uf, r, lat, lon, _ in MUNICIPIOS],
        )
        clima = gerar_clima(agora)
        with cur.copy("COPY silver.clima_diario FROM STDIN") as copia:
            for linha in clima:
                copia.write_row(linha)
    print(f"Silver de amostra: {len(MUNICIPIOS)} municípios, {len(clima)} dias de clima.")


if __name__ == "__main__":
    main()
