"""Clima diário dos municípios brasileiros (Open-Meteo, API de dados históricos).

A carga é incremental: cada execução busca só os dias que ainda faltam para cada
município, a partir do checkpoint. Pode ser rodada quantas vezes quiser.

Executar (com o .venv ativo, na raiz do projeto e o lake no ar):
    python -m ingestion.clima --uf SP --limite 3     # teste rápido: 3 cidades de SP
    python -m ingestion.clima --uf SP                # SP inteiro (só o que falta)
    python -m ingestion.clima --uf SP --inicio 2025-03-01 --fim 2025-03-31 --reprocessar
"""

import argparse
import logging
import math
import time
from datetime import UTC, date, datetime, timedelta
from typing import Any

import pandas as pd
import requests

from ingestion.lake import (
    ConfigLake,
    carregar_config,
    existe,
    gravar_parquet,
    ler_parquet,
    mesclar_parquet,
)
from ingestion.municipios import CAMINHO_MUNICIPIOS, criar_sessao

logger = logging.getLogger(__name__)

URL_OPEN_METEO = "https://archive-api.open-meteo.com/v1/archive"
FONTE_OPEN_METEO = "open_meteo_archive_v1"

# Timeout maior que o do IBGE: uma chamada com muitos municípios e quase dois
# anos de dados demora mais para a API montar.
TIMEOUT_SEGUNDOS = 90

# Variáveis diárias que eu busco. Mantenho no máximo 10, porque a API cobra mais
# "peso" por chamada quando passa disso. O foco do projeto é temperatura.
VARIAVEIS_DIARIAS = [
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "relative_humidity_2m_mean",
    "wind_speed_10m_max",
]

# O histórico do projeto começa em 01/01/2025.
DATA_INICIO = date(2025, 1, 1)

# Os dados de reanálise (ERA5) chegam com cerca de 5 dias de atraso. Se eu pedir
# dias mais recentes que isso, eles viriam vazios, então paro antes.
ATRASO_DIAS = 5

# Limites do plano gratuito: 600 chamadas por minuto e 10.000 por dia, onde cada
# chamada "pesa" locais × (dias / 14) × (variáveis / 10). Eu monto lotes que
# pesam no máximo 500 por minuto, deixando folga em relação aos 600.
PESO_MAXIMO_POR_MINUTO = 500
PAUSA_ENTRE_LOTES_SEGUNDOS = 60

# Lake: uma pasta por ano, mês e UF. A gravação mescla com o que já existe, pela
# chave município + dia, então a carga é incremental e idempotente ao mesmo tempo.
CAMINHO_CLIMA = "bronze/clima_diario"
PARTICOES = ["ano", "mes", "uf_sigla"]
CHAVE_CLIMA = ["codigo_ibge", "data"]

# Checkpoint: a última data com dado válido de cada município. É por ele que a
# carga sabe o que já foi feito e retoma de onde parou se for interrompida.
CAMINHO_CHECKPOINT = "controle/checkpoint_clima.parquet"

# Variável que uso para decidir se um dia "chegou de verdade". Dias recentes podem
# vir com valores nulos enquanto a reanálise não fica pronta; esses dias eu não
# marco como feitos, para buscá-los de novo na próxima execução.
VARIAVEL_REFERENCIA = "temperature_2m_mean"

# Faixas plausíveis para o Brasil. Valores fora delas viram aviso para investigar.
FAIXA_TEMPERATURA = (-15.0, 50.0)

SCHEMA_CLIMA = {
    "codigo_ibge": "int64",
    "uf_sigla": "string",
    "temperature_2m_max": "float64",
    "temperature_2m_min": "float64",
    "temperature_2m_mean": "float64",
    "precipitation_sum": "float64",
    "relative_humidity_2m_mean": "float64",
    "wind_speed_10m_max": "float64",
}


def periodo_padrao(hoje: date | None = None) -> tuple[date, date]:
    """Período completo da carga: de DATA_INICIO até o último dia já disponível."""
    hoje = hoje or date.today()
    return DATA_INICIO, hoje - timedelta(days=ATRASO_DIAS)


def peso_por_local(inicio: date, fim: date, n_variaveis: int = len(VARIAVEIS_DIARIAS)) -> float:
    """Estima quanto cada município "pesa" no limite da API para o período pedido."""
    # A API conta no mínimo 14 dias e 10 variáveis por chamada, por isso os max().
    n_dias = (fim - inicio).days + 1
    return max(n_dias / 14, 1.0) * max(n_variaveis / 10, 1.0)


def tamanho_do_lote(inicio: date, fim: date) -> int:
    """Quantos municípios cabem em uma chamada sem passar do peso por minuto."""
    # Pelo menos 1, mesmo que um único município já passe do limite (períodos
    # muito longos): nesse caso a pausa entre lotes é o que segura o ritmo.
    return max(1, math.floor(PESO_MAXIMO_POR_MINUTO / peso_por_local(inicio, fim)))


def extrair_clima(
    lote: pd.DataFrame,
    inicio: date,
    fim: date,
    sessao: requests.Session,
) -> list[dict[str, Any]]:
    """Busca o clima diário de um lote de municípios em uma única chamada."""
    # A API aceita vários pontos de uma vez, com latitudes e longitudes separadas
    # por vírgula. Arredondo para 4 casas (~11 m), precisão de sobra para clima.
    parametros = {
        "latitude": ",".join(f"{v:.4f}" for v in lote["latitude"]),
        "longitude": ",".join(f"{v:.4f}" for v in lote["longitude"]),
        "start_date": inicio.isoformat(),
        "end_date": fim.isoformat(),
        "daily": ",".join(VARIAVEIS_DIARIAS),
        # "auto" faz a API usar o fuso de cada ponto. Assim o "dia" de Rio Branco
        # (UTC-5) e o de Brasília (UTC-3) começam à meia-noite local de cada um.
        "timezone": "auto",
    }

    logger.info("Open-Meteo: %d município(s), %s a %s", len(lote), inicio, fim)
    resposta = sessao.get(URL_OPEN_METEO, params=parametros, timeout=TIMEOUT_SEGUNDOS)
    resposta.raise_for_status()
    dados = resposta.json()

    # Com um único ponto a API devolve um objeto; com vários, uma lista. Eu
    # padronizo sempre como lista para o resto do código não ter dois caminhos.
    if isinstance(dados, dict):
        dados = [dados]

    # A resposta vem na mesma ordem dos pontos enviados. Se a quantidade não bater,
    # não dá para saber qual resultado é de qual município: prefiro parar.
    if len(dados) != len(lote):
        raise ValueError(
            f"Pedi {len(lote)} município(s) e a API devolveu {len(dados)} resultado(s)."
        )
    return dados


def transformar_clima(
    respostas: list[dict[str, Any]],
    lote: pd.DataFrame,
    extraido_em: datetime,
) -> pd.DataFrame:
    """Converte as respostas da API em uma tabela: uma linha por município e dia.

    Função pura (sem rede nem disco), testável com respostas de exemplo.
    """
    tabelas = []
    # Uso zip para casar cada resposta com o município do lote, pela posição.
    for resposta, municipio in zip(respostas, lote.itertuples(index=False), strict=True):
        diario = resposta.get("daily")
        if not diario or "time" not in diario:
            raise ValueError(
                f"Resposta sem dados diários para o município {municipio.codigo_ibge}."
            )

        faltando = set(VARIAVEIS_DIARIAS) - set(diario)
        if faltando:
            raise ValueError(f"A resposta não tem as variáveis esperadas: {sorted(faltando)}")

        tabela = pd.DataFrame({coluna: diario[coluna] for coluna in ["time", *VARIAVEIS_DIARIAS]})
        tabela["codigo_ibge"] = municipio.codigo_ibge
        tabela["uf_sigla"] = municipio.uf_sigla
        tabelas.append(tabela)

    df = pd.concat(tabelas, ignore_index=True)

    # Converto o texto "2025-01-31" em data e derivo ano e mês para as partições.
    datas = pd.to_datetime(df.pop("time"), format="%Y-%m-%d")
    df = df.astype(SCHEMA_CLIMA)
    df.insert(1, "data", datas.dt.date)
    df["ano"] = datas.dt.year.astype("int64")
    df["mes"] = datas.dt.month.astype("int64")

    df["_fonte"] = FONTE_OPEN_METEO
    df["_extraido_em"] = pd.Timestamp(extraido_em)
    return df


def validar_clima(df: pd.DataFrame, n_municipios: int, inicio: date, fim: date) -> None:
    """Confere a qualidade do clima de uma UF antes de gravar."""
    # --- Problemas críticos: interrompem a carga ---
    if df.empty:
        raise ValueError("A tabela de clima está vazia.")

    duplicados = df.duplicated(subset=["codigo_ibge", "data"])
    if duplicados.any():
        raise ValueError(f"{int(duplicados.sum())} linha(s) duplicada(s) por município e dia.")

    fora_do_periodo = (df["data"] < inicio) | (df["data"] > fim)
    if fora_do_periodo.any():
        raise ValueError(f"{int(fora_do_periodo.sum())} linha(s) fora do período {inicio} a {fim}.")

    # --- Problemas pontuais: viram aviso, sem descartar nada ---
    dias_esperados = (fim - inicio).days + 1
    esperado = n_municipios * dias_esperados
    if len(df) != esperado:
        logger.warning(
            "Esperava %d linhas (%d municípios × %d dias) e recebi %d.",
            esperado,
            n_municipios,
            dias_esperados,
            len(df),
        )

    nulos = df[VARIAVEIS_DIARIAS].isna().mean()
    for variavel, proporcao in nulos[nulos > 0].items():
        logger.warning("'%s' com %.2f%% de valores nulos.", variavel, proporcao * 100)

    minimo, maximo = FAIXA_TEMPERATURA
    temperaturas = df[["temperature_2m_max", "temperature_2m_min", "temperature_2m_mean"]]
    estranhas = ((temperaturas < minimo) | (temperaturas > maximo)).any(axis=1)
    if estranhas.any():
        exemplos = df.loc[estranhas, ["codigo_ibge", "data"]].head(5).to_dict("records")
        logger.warning(
            "%d dia(s) com temperatura fora de %s °C. Exemplos: %s",
            int(estranhas.sum()),
            FAIXA_TEMPERATURA,
            exemplos,
        )

    logger.info(
        "Validação do clima concluída: %d linhas, %d municípios.",
        len(df),
        df["codigo_ibge"].nunique(),
    )


def _checkpoint_vazio() -> pd.DataFrame:
    return pd.DataFrame(
        {"codigo_ibge": pd.Series(dtype="int64"), "ultima_data": pd.Series(dtype="object")}
    )


def ler_checkpoint(config: ConfigLake) -> pd.DataFrame:
    """Lê o checkpoint do lake. Na primeira carga, devolve uma tabela vazia."""
    if not existe(CAMINHO_CHECKPOINT, config):
        return _checkpoint_vazio()
    return ler_parquet(CAMINHO_CHECKPOINT, config)[["codigo_ibge", "ultima_data"]]


def atualizar_checkpoint(df_novo: pd.DataFrame, config: ConfigLake) -> None:
    """Registra, para cada município do lote, a última data com dado válido."""
    # Só considero "feito" o dia que veio com valor. Um dia que veio nulo será
    # pedido de novo na próxima execução, quando a API já tiver o dado.
    validos = df_novo[df_novo[VARIAVEL_REFERENCIA].notna()]
    if validos.empty:
        logger.warning("Nenhum dia com dado válido neste lote; checkpoint não avançou.")
        return

    novos = validos.groupby("codigo_ibge", as_index=False)["data"].max()
    novos = novos.rename(columns={"data": "ultima_data"})

    # Junto ao checkpoint anterior e fico com a maior data de cada município. Uso a
    # maior (e não a mais recente gravada) para um reprocessamento de um período
    # antigo nunca fazer o checkpoint "andar para trás".
    todos = pd.concat([ler_checkpoint(config), novos], ignore_index=True)
    checkpoint = todos.groupby("codigo_ibge", as_index=False)["ultima_data"].max()
    checkpoint["atualizado_em"] = pd.Timestamp(datetime.now(UTC))

    gravar_parquet(checkpoint, CAMINHO_CHECKPOINT, config)


def calcular_pendencias(
    municipios: pd.DataFrame,
    checkpoint: pd.DataFrame,
    inicio: date,
    fim: date,
) -> pd.DataFrame:
    """Diz, para cada município, a partir de que dia ainda falta buscar.

    Função pura. Devolve os municípios com a coluna "inicio_pendente" e só os
    que ainda têm algo a buscar até "fim".
    """
    df = municipios.merge(checkpoint[["codigo_ibge", "ultima_data"]], on="codigo_ibge", how="left")

    # Nunca carregado: começa no início do período. Já carregado: começa no dia
    # seguinte ao último. Uso o maior dos dois para respeitar um --inicio pedido.
    proximo_dia = df["ultima_data"].map(lambda d: d + timedelta(days=1) if pd.notna(d) else inicio)
    df["inicio_pendente"] = proximo_dia.map(lambda d: max(d, inicio))

    pendentes = df[df["inicio_pendente"] <= fim].drop(columns="ultima_data")
    return pendentes.reset_index(drop=True)


def carregar_clima_uf(
    municipios_uf: pd.DataFrame,
    inicio: date,
    fim: date,
    sessao: requests.Session,
    config: ConfigLake,
    pausa_segundos: float = PAUSA_ENTRE_LOTES_SEGUNDOS,
    reprocessar: bool = False,
) -> int:
    """Busca e grava o clima que falta para os municípios de uma UF.

    Grava e atualiza o checkpoint a cada lote, para que uma interrupção (limite
    da API, queda de rede) perca no máximo o lote em andamento.
    Devolve o número de linhas novas buscadas na API.
    """
    if municipios_uf.empty:
        logger.warning("Nenhum município recebido para carregar.")
        return 0

    uf = municipios_uf["uf_sigla"].iloc[0]

    # Não dá para buscar clima sem coordenada: deixo esses municípios de fora,
    # mas registro quais foram, em vez de sumir com eles em silêncio.
    sem_coordenada = municipios_uf[["latitude", "longitude"]].isna().any(axis=1)
    if sem_coordenada.any():
        logger.warning(
            "%s: %d município(s) sem coordenada ficaram de fora: %s",
            uf,
            int(sem_coordenada.sum()),
            municipios_uf.loc[sem_coordenada, "nome"].tolist(),
        )
    municipios_uf = municipios_uf.loc[~sem_coordenada]

    # Com --reprocessar eu ignoro o checkpoint e busco o período inteiro de novo,
    # útil para corrigir um intervalo. A mesclagem no lake evita duplicar linhas.
    checkpoint = _checkpoint_vazio() if reprocessar else ler_checkpoint(config)
    pendentes = calcular_pendencias(municipios_uf, checkpoint, inicio, fim)
    if pendentes.empty:
        logger.info("%s: tudo em dia até %s. Nada a buscar.", uf, fim)
        return 0

    # Municípios com o mesmo dia de início podem ir juntos na mesma chamada. Em
    # geral é um grupo só; mais de um aparece quando uma carga anterior parou no meio.
    total = 0
    primeira_chamada = True
    for inicio_grupo, grupo in pendentes.groupby("inicio_pendente", sort=True):
        tamanho = tamanho_do_lote(inicio_grupo, fim)
        n_lotes = math.ceil(len(grupo) / tamanho)
        logger.info(
            "%s: %d município(s) a partir de %s, em %d lote(s) de até %d.",
            uf,
            len(grupo),
            inicio_grupo,
            n_lotes,
            tamanho,
        )

        for numero, posicao in enumerate(range(0, len(grupo), tamanho), start=1):
            # Respeito o limite por minuto da API com uma pausa entre as chamadas.
            if not primeira_chamada and pausa_segundos > 0:
                logger.info("Pausa de %.0f s para respeitar o limite da API.", pausa_segundos)
                time.sleep(pausa_segundos)
            primeira_chamada = False

            lote = grupo.iloc[posicao : posicao + tamanho]
            extraido_em = datetime.now(UTC)
            respostas = extrair_clima(lote, inicio_grupo, fim, sessao)
            df = transformar_clima(respostas, lote, extraido_em)
            validar_clima(df, len(lote), inicio_grupo, fim)

            # Primeiro gravo os dados, depois o checkpoint. Se algo falhar entre os
            # dois, o pior caso é buscar o lote de novo, nunca perder dados.
            mesclar_parquet(df, CAMINHO_CLIMA, config, PARTICOES, CHAVE_CLIMA)
            atualizar_checkpoint(df, config)

            total += len(df)
            logger.info("%s: lote %d/%d gravado (%d linhas).", uf, numero, n_lotes, len(df))

    return total


def carregar_clima(
    ufs: list[str],
    inicio: date,
    fim: date,
    limite: int | None = None,
    reprocessar: bool = False,
    config: ConfigLake | None = None,
    sessao: requests.Session | None = None,
) -> None:
    """Carrega o clima que falta para as UFs pedidas, uma UF por vez."""
    if inicio > fim:
        raise ValueError(f"Data inicial {inicio} é posterior à final {fim}.")

    config = config or carregar_config()
    # Mais tentativas e esperas maiores que o padrão: o limite da API é por
    # minuto, então o retry precisa conseguir esperar até o minuto virar.
    sessao = sessao or criar_sessao(tentativas=5, backoff=2.0)

    municipios = ler_parquet(CAMINHO_MUNICIPIOS, config)
    ufs = [uf.upper() for uf in ufs]
    desconhecidas = set(ufs) - set(municipios["uf_sigla"].astype(str))
    if desconhecidas:
        raise ValueError(
            f"UF(s) não encontrada(s) na tabela de municípios: {sorted(desconhecidas)}"
        )

    for uf in ufs:
        municipios_uf = municipios[municipios["uf_sigla"].astype(str) == uf]
        municipios_uf = municipios_uf.sort_values("codigo_ibge")
        if limite:
            # Modo de teste: só as primeiras N cidades da UF.
            municipios_uf = municipios_uf.head(limite)

        try:
            linhas = carregar_clima_uf(
                municipios_uf, inicio, fim, sessao, config, reprocessar=reprocessar
            )
        except requests.exceptions.RetryError:
            # As tentativas se esgotaram, o que costuma ser o limite diário da API.
            # O que já foi gravado está salvo no checkpoint: é só rodar de novo depois.
            logger.error(
                "%s: limite da API atingido. O progresso foi salvo; rode de novo mais tarde "
                "que a carga continua de onde parou.",
                uf,
            )
            raise
        logger.info("%s concluída: %d linha(s) nova(s) em %s.", uf, linhas, CAMINHO_CLIMA)


def _ler_argumentos() -> argparse.Namespace:
    padrao_inicio, padrao_fim = periodo_padrao()
    parser = argparse.ArgumentParser(description="Carrega o clima diário do Open-Meteo no lake.")
    parser.add_argument("--uf", nargs="+", required=True, help="Uma ou mais UFs, ex.: SP RJ")
    parser.add_argument(
        "--inicio",
        type=date.fromisoformat,
        default=padrao_inicio,
        help=f"Data inicial AAAA-MM-DD (padrão: {padrao_inicio})",
    )
    parser.add_argument(
        "--fim",
        type=date.fromisoformat,
        default=padrao_fim,
        help=f"Data final AAAA-MM-DD (padrão: hoje menos {ATRASO_DIAS} dias, {padrao_fim})",
    )
    parser.add_argument(
        "--limite", type=int, default=None, help="Só as primeiras N cidades de cada UF (testes)"
    )
    parser.add_argument(
        "--reprocessar",
        action="store_true",
        help="Ignora o checkpoint e busca o período inteiro de novo",
    )
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    argumentos = _ler_argumentos()
    carregar_clima(
        argumentos.uf,
        argumentos.inicio,
        argumentos.fim,
        argumentos.limite,
        argumentos.reprocessar,
    )
