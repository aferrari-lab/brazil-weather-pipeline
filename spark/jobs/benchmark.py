"""Comparativo de performance: pandas × PySpark na transformação da silver.

Mede, em vários tamanhos de dados, o tempo de ponta a ponta (ler o bronze →
transformar → gravar a silver) nas duas ferramentas, e o efeito do particionamento
na leitura: buscar uma UF e um mês num lake sem partições (precisa ler tudo) × no
lake particionado por ano/mês/UF (lê só a pasta certa).

Os dados são sintéticos, no mesmo formato do bronze real, para eu poder testar
volumes maiores do que os que já carreguei. Antes de medir, confiro que pandas e
Spark chegam ao mesmo resultado: sem isso a comparação não valeria nada.

Executar (com o ambiente no ar):
    docker compose run --rm spark /opt/spark/bin/spark-submit --driver-memory 4g benchmark.py
    docker compose run --rm spark /opt/spark/bin/spark-submit benchmark.py --tamanhos 100000 1000000
"""

import argparse
import shutil
import statistics
import tempfile
import time
from collections.abc import Callable
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession

import silver_clima

# Onde os resultados são salvos. No container, docs/ é a pasta docs do projeto.
PASTA_RESULTADOS = Path("/opt/spark/docs/benchmark")

N_MUNICIPIOS = 5570
# Siglas reais, para as partições por UF ficarem parecidas com as do lake.
UFS = [
    "AC",
    "AL",
    "AM",
    "AP",
    "BA",
    "CE",
    "DF",
    "ES",
    "GO",
    "MA",
    "MG",
    "MS",
    "MT",
    "PA",
    "PB",
    "PE",
    "PI",
    "PR",
    "RJ",
    "RN",
    "RO",
    "RR",
    "RS",
    "SC",
    "SE",
    "SP",
    "TO",
]
# Proporção de linhas repetidas (mesmo município e dia, extração mais nova), para a
# deduplicação ter trabalho de verdade, como acontece com recargas no bronze.
PROPORCAO_DUPLICADAS = 0.02
REPETICOES = 3


# --- Dados sintéticos ---------------------------------------------------------------
def gerar_bronze(n_linhas: int, semente: int = 42) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Gera um bronze de clima e um cadastro de municípios sintéticos."""
    gerador = np.random.default_rng(semente)
    codigos = np.arange(1_000_000, 1_000_000 + N_MUNICIPIOS)
    ufs = np.array([UFS[i % len(UFS)] for i in range(N_MUNICIPIOS)])

    n_dias = -(-n_linhas // N_MUNICIPIOS)  # divisão arredondando para cima
    dias = pd.date_range("2025-01-01", periods=n_dias, freq="D")

    # Combinação município × dia, cortada no tamanho pedido.
    indice_municipio = np.tile(np.arange(N_MUNICIPIOS), n_dias)[:n_linhas]
    indice_dia = np.repeat(np.arange(n_dias), N_MUNICIPIOS)[:n_linhas]
    datas = dias[indice_dia]

    media = gerador.normal(24, 5, n_linhas).round(1)
    amplitude = gerador.uniform(4, 14, n_linhas).round(1)
    bronze = pd.DataFrame(
        {
            "codigo_ibge": codigos[indice_municipio],
            "uf_sigla": ufs[indice_municipio],
            "data": datas.date,
            "ano": datas.year.astype("int32"),
            "mes": datas.month.astype("int32"),
            "temperature_2m_max": (media + amplitude / 2).round(1),
            "temperature_2m_min": (media - amplitude / 2).round(1),
            "temperature_2m_mean": media,
            "precipitation_sum": gerador.exponential(3, n_linhas).round(1),
            "relative_humidity_2m_mean": gerador.uniform(40, 95, n_linhas).round(0),
            "wind_speed_10m_max": gerador.uniform(5, 40, n_linhas).round(1),
            "_fonte": "sintetico",
            "_extraido_em": pd.Timestamp("2026-10-01", tz="UTC"),
            "_gravado_em": pd.Timestamp("2026-10-01 00:01", tz="UTC"),
        }
    )

    # Repito uma parte das linhas como "recarga", com extração mais nova e valor
    # diferente: a deduplicação precisa ficar com estas.
    repetidas = bronze.sample(frac=PROPORCAO_DUPLICADAS, random_state=semente).copy()
    repetidas["temperature_2m_max"] += 0.5
    repetidas["_extraido_em"] = pd.Timestamp("2026-10-02", tz="UTC")
    repetidas["_gravado_em"] = pd.Timestamp("2026-10-02 00:01", tz="UTC")
    bronze = pd.concat([bronze, repetidas], ignore_index=True)

    municipios = pd.DataFrame(
        {
            "codigo_ibge": codigos,
            "nome": [f"Municipio {c}" for c in codigos],
            "uf_sigla": ufs,
            "regiao": "Sintetica",
        }
    )
    return bronze, municipios


# --- A mesma transformação da silver, em pandas ------------------------------------
def transformar_pandas(bronze: pd.DataFrame, municipios: pd.DataFrame) -> pd.DataFrame:
    """Versão pandas de silver_clima.transformar, com as mesmas regras."""
    clima = bronze.rename(columns=silver_clima.RENOMEAR)

    # Deduplicação: ordeno da gravação mais nova para a mais antiga e fico com a
    # primeira de cada município e dia (equivale ao row_number do Spark).
    clima = clima.sort_values(["_gravado_em", "_extraido_em"], ascending=False).drop_duplicates(
        subset=silver_clima.CHAVE, keep="first"
    )

    cadastro = municipios[["codigo_ibge", "nome", "regiao"]].rename(columns={"nome": "municipio"})
    clima = clima.merge(cadastro, on="codigo_ibge", how="left")

    clima["amplitude_termica_c"] = (clima["temp_max_c"] - clima["temp_min_c"]).round(1)
    clima["dado_completo"] = clima[silver_clima.MEDIDAS].notna().all(axis=1)

    minimo, maximo = silver_clima.TEMP_MINIMA_C, silver_clima.TEMP_MAXIMA_C
    folga = silver_clima.TOLERANCIA_C
    temperaturas = clima[["temp_max_c", "temp_min_c", "temp_media_c"]]
    fora_da_faixa = ((temperaturas < minimo) | (temperaturas > maximo)).any(axis=1)
    incoerente = (clima["temp_min_c"] > clima["temp_max_c"] + folga) | ~clima[
        "temp_media_c"
    ].between(clima["temp_min_c"] - folga, clima["temp_max_c"] + folga)
    clima["temp_suspeita"] = fora_da_faixa | incoerente
    clima["_processado_em"] = pd.Timestamp(datetime.now()).tz_localize("UTC")

    return clima[silver_clima.COLUNAS_SILVER]


# --- Medição -------------------------------------------------------------------------
def cronometrar(funcao: Callable[[], object], repeticoes: int = REPETICOES) -> float:
    """Roda a função algumas vezes e devolve a mediana do tempo, em segundos."""
    # Uso a mediana, e não a média, para uma execução fora da curva (o sistema
    # ocupado com outra coisa naquele instante) não distorcer o resultado.
    tempos = []
    for _ in range(repeticoes):
        inicio = time.perf_counter()
        funcao()
        tempos.append(time.perf_counter() - inicio)
    return statistics.median(tempos)


def medir_tamanho(spark: SparkSession, n_linhas: int, pasta: Path) -> list[dict]:
    """Mede pandas e Spark para um tamanho de dados. Devolve uma linha por medição."""
    bronze, municipios = gerar_bronze(n_linhas)
    caminho_bronze, caminho_municipios = pasta / "bronze", pasta / "municipios.parquet"
    caminho_plano = pasta / "bronze_sem_particoes"
    bronze.to_parquet(caminho_bronze, partition_cols=silver_clima.PARTICOES, index=False)
    # Uma cópia dos mesmos dados sem partições, para medir o que o particionamento economiza.
    caminho_plano.mkdir()
    bronze.to_parquet(caminho_plano / "dados.parquet", index=False, row_group_size=100_000)
    municipios.to_parquet(caminho_municipios, index=False)
    linhas_bronze = len(bronze)
    del bronze  # libero a memória antes de medir

    def pandas_ponta_a_ponta():
        lido = pd.read_parquet(caminho_bronze)
        silver = transformar_pandas(lido, pd.read_parquet(caminho_municipios))
        silver.to_parquet(
            pasta / "silver_pandas",
            partition_cols=silver_clima.PARTICOES,
            index=False,
            existing_data_behavior="delete_matching",
        )
        return silver

    def spark_ponta_a_ponta():
        silver = silver_clima.transformar(
            spark.read.parquet(str(caminho_bronze)),
            spark.read.parquet(str(caminho_municipios)),
        )
        silver.write.mode("overwrite").partitionBy(*silver_clima.PARTICOES).parquet(
            str(pasta / "silver_spark")
        )

    # Busca de uma única UF num único mês (SP, janeiro de 2025), nos dois formatos.
    filtro = [("uf_sigla", "=", "SP"), ("ano", "=", 2025), ("mes", "=", 1)]
    condicao_spark = "uf_sigla = 'SP' AND ano = 2025 AND mes = 1"

    def pandas_sem_particoes():
        # Sem pastas por UF e mês, não há como pular dados: tudo é lido e filtrado.
        return len(pd.read_parquet(caminho_plano, filters=filtro))

    def pandas_com_particoes():
        # Com o filtro nas colunas de partição, o pyarrow abre só a pasta certa.
        return len(pd.read_parquet(caminho_bronze, filters=filtro))

    def spark_sem_particoes():
        return spark.read.parquet(str(caminho_plano)).filter(condicao_spark).count()

    def spark_com_particoes():
        # O Spark aplica o filtro nas pastas antes de ler ("partition pruning").
        return spark.read.parquet(str(caminho_bronze)).filter(condicao_spark).count()

    # Aquecimento: a primeira execução do Spark inclui iniciar coisas internas da JVM.
    spark_ponta_a_ponta()
    conferir_equivalencia(pandas_ponta_a_ponta(), spark.read.parquet(str(pasta / "silver_spark")))

    medicoes = {
        ("ponta_a_ponta", "pandas"): pandas_ponta_a_ponta,
        ("ponta_a_ponta", "spark"): spark_ponta_a_ponta,
        ("busca_sem_particoes", "pandas"): pandas_sem_particoes,
        ("busca_sem_particoes", "spark"): spark_sem_particoes,
        ("busca_com_particoes", "pandas"): pandas_com_particoes,
        ("busca_com_particoes", "spark"): spark_com_particoes,
    }
    resultados = []
    for (cenario, ferramenta), funcao in medicoes.items():
        segundos = cronometrar(funcao)
        print(f"  {cenario:<22} {ferramenta:<7} {segundos:8.2f} s")
        resultados.append(
            {
                "linhas_bronze": linhas_bronze,
                "cenario": cenario,
                "ferramenta": ferramenta,
                "segundos": round(segundos, 3),
            }
        )
    return resultados


def conferir_equivalencia(silver_pandas: pd.DataFrame, silver_spark) -> None:
    """Garante que as duas ferramentas produziram a mesma silver."""
    resumo_spark = silver_spark.selectExpr(
        "count(*) AS linhas",
        "round(sum(temp_max_c), 1) AS soma_temp_max",
        "sum(cast(temp_suspeita AS int)) AS suspeitas",
        "sum(cast(dado_completo AS int)) AS completas",
    ).first()
    resumo_pandas = {
        "linhas": len(silver_pandas),
        "soma_temp_max": round(float(silver_pandas["temp_max_c"].sum()), 1),
        "suspeitas": int(silver_pandas["temp_suspeita"].sum()),
        "completas": int(silver_pandas["dado_completo"].sum()),
    }
    for chave, valor in resumo_pandas.items():
        # A soma de números decimais pode diferir na última casa pela ordem das
        # somas; aceito uma diferença mínima só nela.
        diferenca = abs(float(resumo_spark[chave]) - float(valor))
        tolerancia = 0.5 if chave == "soma_temp_max" else 0
        if diferenca > tolerancia:
            raise AssertionError(
                f"pandas e Spark divergiram em '{chave}': {valor} × {resumo_spark[chave]}"
            )
    print(f"  equivalência conferida: {resumo_pandas['linhas']} linhas iguais nas duas")


# --- Relatório -----------------------------------------------------------------------
def montar_relatorio(resultados: pd.DataFrame, nucleos: int, data_execucao: date) -> str:
    """Monta o relatório em Markdown: tabela + gráfico (Mermaid, que o GitHub desenha)."""
    tabela = resultados.pivot_table(
        index=["linhas_bronze", "cenario"], columns="ferramenta", values="segundos"
    ).reset_index()
    tabela["spark_mais_rapido"] = (tabela["pandas"] / tabela["spark"]).round(2)
    # Ordem de leitura: primeiro o ponta a ponta, depois as buscas.
    ordem = {"ponta_a_ponta": 0, "busca_sem_particoes": 1, "busca_com_particoes": 2}
    tabela = tabela.sort_values(
        ["linhas_bronze", "cenario"], key=lambda c: c.map(ordem) if c.name == "cenario" else c
    )

    linhas = [
        "# Benchmark: pandas × PySpark",
        "",
        f"Executado em {data_execucao:%d/%m/%Y}, Spark local com {nucleos} núcleo(s), "
        f"mediana de {REPETICOES} execuções. Dados sintéticos no formato do bronze real "
        f"({PROPORCAO_DUPLICADAS:.0%} de linhas repetidas para a deduplicação).",
        "",
        "| Linhas no bronze | Cenário | pandas (s) | Spark (s) | pandas ÷ Spark |",
        "| ---: | --- | ---: | ---: | ---: |",
    ]
    for _, r in tabela.iterrows():
        linhas.append(
            f"| {int(r['linhas_bronze']):,} | {r['cenario']} | {r['pandas']:.2f} | "
            f"{r['spark']:.2f} | {r['spark_mais_rapido']:.2f}× |".replace(",", ".")
        )

    # Ganho do particionamento dentro de cada ferramenta: quantas vezes mais rápido
    # é buscar uma UF e um mês no lake particionado do que no lake sem partições.
    busca = (
        resultados[resultados["cenario"].str.startswith("busca_")]
        .pivot_table(index=["linhas_bronze", "ferramenta"], columns="cenario", values="segundos")
        .reset_index()
    )
    linhas += [
        "",
        "**Ganho do particionamento** (buscar SP em janeiro de 2025):",
        "",
        "| Linhas no bronze | Ferramenta | Sem partições (s) | Com partições (s) | Ganho |",
        "| ---: | --- | ---: | ---: | ---: |",
    ]
    for _, r in busca.iterrows():
        ganho = r["busca_sem_particoes"] / r["busca_com_particoes"]
        linhas.append(
            f"| {int(r['linhas_bronze']):,} | {r['ferramenta']} | "
            f"{r['busca_sem_particoes']:.2f} | {r['busca_com_particoes']:.2f} | "
            f"{ganho:.1f}× |".replace(",", ".")
        )

    ponta = tabela[tabela["cenario"] == "ponta_a_ponta"].sort_values("linhas_bronze")
    rotulos = ", ".join(f'"{int(n / 1000):,}k"'.replace(",", ".") for n in ponta["linhas_bronze"])
    teto = float(max(ponta["pandas"].max(), ponta["spark"].max())) * 1.15
    linhas += [
        "",
        'Valores acima de 1× em "pandas ÷ Spark" significam que o Spark foi mais rápido.',
        "",
        "```mermaid",
        "xychart-beta",
        '    title "Ponta a ponta (ler, transformar, gravar), em segundos"',
        f"    x-axis [{rotulos}]",
        f'    y-axis "segundos" 0 --> {teto:.0f}',
        f"    bar [{', '.join(f'{v:.2f}' for v in ponta['pandas'])}]",
        f"    line [{', '.join(f'{v:.2f}' for v in ponta['spark'])}]",
        "```",
        "",
        "Barras: pandas. Linha: Spark.",
    ]
    return "\n".join(linhas) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark pandas × PySpark.")
    parser.add_argument("--tamanhos", type=int, nargs="+", default=[100_000, 1_000_000, 3_000_000])
    parser.add_argument("--saida", type=Path, default=PASTA_RESULTADOS)
    argumentos = parser.parse_args()

    # Spark local, sem o lake: o benchmark usa pastas temporárias dentro do container,
    # para medir só o processamento, sem a rede até o RustFS no meio.
    spark = (
        SparkSession.builder.appName("benchmark")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.debug.maxToStringFields", "200")
        # Sem a barra de progresso do Spark, para a saída do benchmark ficar legível.
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    nucleos = spark.sparkContext.defaultParallelism

    resultados = []
    for n_linhas in sorted(argumentos.tamanhos):
        print(f"\n=== {n_linhas:,} linhas ===".replace(",", "."))
        pasta = Path(tempfile.mkdtemp(prefix="benchmark_"))
        try:
            resultados += medir_tamanho(spark, n_linhas, pasta)
        finally:
            shutil.rmtree(pasta, ignore_errors=True)
    spark.stop()

    argumentos.saida.mkdir(parents=True, exist_ok=True)
    tabela = pd.DataFrame(resultados)
    tabela.to_csv(argumentos.saida / "resultados.csv", index=False)
    relatorio = montar_relatorio(tabela, nucleos, date.today())
    (argumentos.saida / "README.md").write_text(relatorio, encoding="utf-8")
    print(f"\nResultados salvos em {argumentos.saida}/ (resultados.csv e README.md)")


if __name__ == "__main__":
    main()
