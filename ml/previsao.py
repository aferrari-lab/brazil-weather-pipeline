"""Previsão da temperatura média de 1 dia à frente, por município.

Lê as features do dbt (ml.ml_features_temperatura), treina um modelo de gradient
boosting e compara com dois palpites simples, que todo modelo precisa bater para
valer a pena:
- persistência: o dia seguinte igual à véspera (em temperatura, difícil de bater);
- média sazonal: a média do município naquele mês, no período de treino.

A avaliação é temporal: treino com tudo até 60 dias antes do último dado e testo
nesses 60 dias finais, que o modelo nunca viu. Embaralhar as datas daria um erro
otimista, porque o modelo "veria o futuro" de cada dia no treino.

Depois de avaliar, retreino com o histórico inteiro e prevejo o dia seguinte ao
último dado de cada município. Essa previsão é publicada uma vez e nunca
reescrita: quando o dado real chegar, a comparação é honesta.

Grava no Postgres (schema ml):
- avaliacao_modelos: MAE e RMSE de cada modelo em cada treino (histórico);
- previsoes_teste: previsto × real nos dias de teste do último treino;
- previsoes_publicadas: a previsão do próximo dia de cada execução.

Executar (com o ambiente no ar e o dbt já rodado):
    docker compose run --rm ml
"""

import argparse
import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime

import numpy as np
import pandas as pd
import psycopg
from sklearn.ensemble import HistGradientBoostingRegressor

logger = logging.getLogger("previsao")

VERSAO_MODELO = "hgb-v1"
TABELA_FEATURES = "ml.ml_features_temperatura"
ALVO = "alvo_temp_media_c"
FEATURES = [
    "temp_media_d1",
    "temp_media_d2",
    "temp_media_d3",
    "temp_media_d7",
    "temp_media_7d_anteriores",
    "temp_max_d1",
    "temp_min_d1",
    "chuva_d1",
    "umidade_d1",
    "vento_d1",
    "sazonal_seno",
    "sazonal_cosseno",
    "latitude",
    "longitude",
]
DIAS_TESTE = 60
# Com o Brasil inteiro, o histórico passa de 3 milhões de linhas. Para o treino
# caber folgado na memória do PC, uso uma amostra aleatória acima deste limite.
MAX_LINHAS_TREINO = 1_500_000
SEMENTE = 42


@dataclass
class Resultado:
    avaliacao: pd.DataFrame
    previsoes_teste: pd.DataFrame
    previsoes_futuras: pd.DataFrame


# --- Preparação ---
def separar_conjuntos(
    df: pd.DataFrame, dias_teste: int = DIAS_TESTE
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Separa treino, teste (os últimos dias com dado) e futuro (a prever)."""
    historico = df[df["conjunto"] == "historico"]
    futuro = df[df["conjunto"] == "futuro"]
    if historico.empty:
        raise ValueError("Não há histórico nas features: rode o dbt antes.")

    corte = historico["data"].max() - pd.Timedelta(days=dias_teste - 1)
    treino = historico[historico["data"] < corte]
    teste = historico[historico["data"] >= corte]
    if treino.empty:
        raise ValueError(
            f"Histórico curto demais para separar {dias_teste} dias de teste "
            f"(vai de {historico['data'].min():%Y-%m-%d} a {historico['data'].max():%Y-%m-%d})."
        )
    return treino, teste, futuro


def amostrar(treino: pd.DataFrame, maximo: int = MAX_LINHAS_TREINO) -> pd.DataFrame:
    if len(treino) <= maximo:
        return treino
    logger.info("Treino com %d linhas: uso uma amostra de %d.", len(treino), maximo)
    return treino.sample(n=maximo, random_state=SEMENTE)


# --- Palpites de referência (baselines) ---
def tabela_sazonal(treino: pd.DataFrame) -> pd.DataFrame:
    """Média da temperatura por município e mês, só com dados de treino."""
    return (
        treino.groupby(["codigo_ibge", "mes"], as_index=False)[ALVO]
        .mean()
        .rename(columns={ALVO: "media_sazonal"})
    )


def prever_sazonal(sazonal: pd.DataFrame, df: pd.DataFrame) -> np.ndarray:
    """Média do município naquele mês. Sem esse mês no treino (o histórico ainda é
    curto), uso a média dos 7 dias anteriores, para nunca ficar sem palpite."""
    juntado = df[["codigo_ibge", "mes"]].merge(sazonal, on=["codigo_ibge", "mes"], how="left")
    palpite = juntado["media_sazonal"].to_numpy()
    reserva = df["temp_media_7d_anteriores"].to_numpy()
    return np.where(np.isnan(palpite), reserva, palpite)


# --- Modelo ---
def criar_modelo() -> HistGradientBoostingRegressor:
    # Gradient boosting por histogramas: rápido com milhões de linhas e aceita
    # valores vazios nas features sem precisar preencher.
    return HistGradientBoostingRegressor(
        max_iter=300,
        learning_rate=0.05,
        max_leaf_nodes=31,
        l2_regularization=1.0,
        early_stopping=False,  # sem sorteio de validação: o resultado é reproduzível
        random_state=SEMENTE,
    )


def metricas(real: pd.Series, previsto: np.ndarray) -> dict[str, float]:
    erro = np.asarray(previsto, dtype=float) - real.to_numpy(dtype=float)
    return {
        "mae_c": float(np.mean(np.abs(erro))),
        "rmse_c": float(np.sqrt(np.mean(erro**2))),
    }


def executar(df: pd.DataFrame, agora: datetime, dias_teste: int = DIAS_TESTE) -> Resultado:
    """Avalia os três palpites no teste e prevê o próximo dia de cada município."""
    df = df.assign(data=pd.to_datetime(df["data"]))
    treino, teste, futuro = separar_conjuntos(df, dias_teste)
    logger.info(
        "Treino: %d linhas até %s. Teste: %d linhas de %s a %s.",
        len(treino),
        f"{treino['data'].max():%Y-%m-%d}",
        len(teste),
        f"{teste['data'].min():%Y-%m-%d}",
        f"{teste['data'].max():%Y-%m-%d}",
    )

    # 1) Avaliação: treino → teste.
    amostra = amostrar(treino)
    modelo = criar_modelo().fit(amostra[FEATURES], amostra[ALVO])
    sazonal = tabela_sazonal(treino)
    palpites = {
        "persistencia": teste["temp_media_d1"].to_numpy(),
        "media_sazonal": prever_sazonal(sazonal, teste),
        "gradient_boosting": modelo.predict(teste[FEATURES]),
    }
    avaliacao = pd.DataFrame(
        [
            {
                "treinado_em": agora,
                "modelo": nome,
                "versao": VERSAO_MODELO,
                **metricas(teste[ALVO], previsto),
                "linhas_treino": len(amostra),
                "linhas_teste": len(teste),
                "inicio_teste": teste["data"].min().date(),
                "fim_teste": teste["data"].max().date(),
            }
            for nome, previsto in palpites.items()
        ]
    )
    previsoes_teste = pd.DataFrame(
        {
            "treinado_em": agora,
            "codigo_ibge": teste["codigo_ibge"].to_numpy(),
            "data": teste["data"].dt.date.to_numpy(),
            "real_c": teste[ALVO].to_numpy(),
            "persistencia_c": palpites["persistencia"],
            "media_sazonal_c": palpites["media_sazonal"],
            "modelo_c": palpites["gradient_boosting"],
        }
    )

    # 2) Produção: retreino com o histórico inteiro (o teste inclusive, que é o
    # dado mais recente e mais parecido com o dia a prever) e prevejo o futuro.
    historico = amostrar(pd.concat([treino, teste]))
    final = criar_modelo().fit(historico[FEATURES], historico[ALVO])
    previsoes_futuras = pd.DataFrame(
        {
            "codigo_ibge": futuro["codigo_ibge"].to_numpy(),
            "data_alvo": futuro["data"].dt.date.to_numpy(),
            "previsto_c": final.predict(futuro[FEATURES]) if len(futuro) else [],
            "persistencia_c": futuro["temp_media_d1"].to_numpy(),
            "versao": VERSAO_MODELO,
            "gerado_em": agora,
        }
    )
    return Resultado(avaliacao, previsoes_teste, previsoes_futuras)


# --- Banco ---
SQL_TABELAS = [
    "CREATE SCHEMA IF NOT EXISTS ml",
    """
    CREATE TABLE IF NOT EXISTS ml.avaliacao_modelos (
        treinado_em timestamptz, modelo text, versao text,
        mae_c double precision, rmse_c double precision,
        linhas_treino bigint, linhas_teste bigint, inicio_teste date, fim_teste date
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ml.previsoes_teste (
        treinado_em timestamptz, codigo_ibge bigint, data date, real_c double precision,
        persistencia_c double precision, media_sazonal_c double precision,
        modelo_c double precision
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ml.previsoes_publicadas (
        codigo_ibge bigint, data_alvo date, previsto_c double precision,
        persistencia_c double precision, versao text, gerado_em timestamptz,
        PRIMARY KEY (codigo_ibge, data_alvo)
    )
    """,
]


def conectar(ambiente: Mapping[str, str] | None = None) -> psycopg.Connection:
    ambiente = os.environ if ambiente is None else ambiente
    return psycopg.connect(
        host=ambiente.get("WAREHOUSE_HOST", "postgres"),
        port=int(ambiente.get("WAREHOUSE_PORT", "5432")),
        dbname=ambiente["WAREHOUSE_DB"],
        user=ambiente["WAREHOUSE_USER"],
        password=ambiente["WAREHOUSE_PASSWORD"],
    )


def ler_features(conn: psycopg.Connection) -> pd.DataFrame:
    colunas = ["codigo_ibge", "data", "mes", "conjunto", ALVO, *FEATURES]
    with conn.cursor() as cur:
        cur.execute(f"SELECT {', '.join(colunas)} FROM {TABELA_FEATURES}")
        return pd.DataFrame(cur.fetchall(), columns=colunas)


def _linhas(df: pd.DataFrame) -> list[tuple]:
    """Linhas com tipos do Python (o psycopg não conhece os do numpy) e NULL no lugar de NaN."""

    def nativo(valor):
        if valor is None or (isinstance(valor, float) and np.isnan(valor)):
            return None
        return valor.item() if isinstance(valor, np.generic) else valor

    return [tuple(nativo(v) for v in linha) for linha in df.itertuples(index=False)]


def _copiar(cur: psycopg.Cursor, tabela: str, df: pd.DataFrame) -> None:
    with cur.copy(f"COPY {tabela} ({', '.join(df.columns)}) FROM STDIN") as copia:
        for linha in _linhas(df):
            copia.write_row(linha)


def gravar(conn: psycopg.Connection, resultado: Resultado) -> None:
    """Grava tudo numa transação: ou entra o resultado inteiro, ou nada."""
    with conn.transaction(), conn.cursor() as cur:
        for sql in SQL_TABELAS:
            cur.execute(sql)
        _copiar(cur, "ml.avaliacao_modelos", resultado.avaliacao)
        # O teste vale só para o último treino: substituo o anterior.
        cur.execute("TRUNCATE ml.previsoes_teste")
        _copiar(cur, "ml.previsoes_teste", resultado.previsoes_teste)
        # Previsão publicada nunca é reescrita: a primeira para cada dia é a que fica.
        cur.execute(
            "CREATE TEMP TABLE novas (LIKE ml.previsoes_publicadas INCLUDING DEFAULTS) "
            "ON COMMIT DROP"
        )
        _copiar(cur, "novas", resultado.previsoes_futuras)
        cur.execute(
            "INSERT INTO ml.previsoes_publicadas SELECT * FROM novas "
            "ON CONFLICT (codigo_ibge, data_alvo) DO NOTHING"
        )


def resumo(avaliacao: pd.DataFrame) -> str:
    """Tabela curta para o log, do melhor para o pior."""
    tabela = avaliacao.sort_values("mae_c")[["modelo", "mae_c", "rmse_c"]]
    return tabela.to_string(index=False, float_format=lambda x: f"{x:.2f}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Previsão da temperatura média de 1 dia à frente.")
    parser.add_argument("--dias-teste", type=int, default=DIAS_TESTE)
    argumentos = parser.parse_args()

    with conectar() as conn:
        features = ler_features(conn)
        logger.info("Lidas %d linhas de %s.", len(features), TABELA_FEATURES)
        resultado = executar(features, datetime.now(UTC), argumentos.dias_teste)
        gravar(conn, resultado)

    logger.info("Erro médio no teste (°C):\n%s", resumo(resultado.avaliacao))
    erros = resultado.avaliacao.set_index("modelo")["mae_c"]
    if erros["gradient_boosting"] >= erros["persistencia"]:
        logger.warning("O modelo não bateu a persistência neste treino.")
    logger.info("%d previsão(ões) do próximo dia calculadas.", len(resultado.previsoes_futuras))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    main()
