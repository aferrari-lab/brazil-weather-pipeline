"""Clima diário dos municípios brasileiros (Open-Meteo, API de dados históricos).

A carga é incremental: cada execução busca só os dias que ainda faltam para cada
município, a partir do que já está no bronze. Pode ser rodada quantas vezes quiser,
inclusive várias UFs em paralelo.

Executar (com o .venv ativo, na raiz do projeto e o lake no ar):
    python -m ingestion.clima --uf SP --limite 3     # teste rápido: 3 cidades de SP
    python -m ingestion.clima --uf SP                # SP inteiro (só o que falta)
    python -m ingestion.clima --uf SP --inicio 2025-03-01 --fim 2025-03-31 --reprocessar

No dia a dia quem roda é o Airflow (DAG clima_diario), com carregar_clima_brasil.
"""

import argparse
import logging
import math
import time
import uuid
from collections import Counter
from datetime import UTC, date, datetime, timedelta
from typing import Any

import pandas as pd
import requests

from ingestion.lake import (
    ConfigLake,
    anexar_parquet,
    carregar_config,
    existe,
    ler_parquet,
)
from ingestion.log_cargas import (
    STATUS_LIMITE_API,
    STATUS_SUCESSO,
    Execucao,
    registrar_carga,
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
# Pausa depois de um lote de peso cheio (500). Lotes mais leves esperam menos, na
# mesma proporção: o ritmo fica sempre em até 500 de peso por minuto.
PAUSA_ENTRE_LOTES_SEGUNDOS = 60
# As coordenadas vão na URL da chamada. Com 500 municípios (o lote de uma carga de
# poucos dias) a URL passa de 9 mil caracteres e a API recusa (erro 414). Com 100,
# fica perto de 2 mil, dentro do limite usual de servidores web.
MAX_MUNICIPIOS_POR_CHAMADA = 100

# Lake: uma pasta por ano, mês e UF. O bronze é "append-only": cada lote grava
# arquivos novos e nada é reescrito. Se um dia aparecer mais de uma vez (um lote
# refeito), a silver fica com a versão gravada por último.
CAMINHO_CLIMA = "bronze/clima_diario"
PARTICOES = ["ano", "mes", "uf_sigla"]

# Não existe um arquivo de checkpoint separado: a última data carregada de cada
# município é calculada lendo o próprio bronze. Sem um estado à parte, não há o
# que duas cargas paralelas possam sobrescrever, nem como ele ficar diferente dos
# dados de verdade.

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
    """Quantos municípios cabem em uma chamada sem passar do peso por minuto
    nem do tamanho de URL que a API aceita."""
    # Pelo menos 1, mesmo que um único município já passe do limite (períodos
    # muito longos): nesse caso a pausa entre lotes é o que segura o ritmo.
    pelo_peso = math.floor(PESO_MAXIMO_POR_MINUTO / peso_por_local(inicio, fim))
    return max(1, min(MAX_MUNICIPIOS_POR_CHAMADA, pelo_peso))


def pausa_do_lote(n_municipios: int, inicio: date, fim: date, pausa_cheia: float) -> float:
    """Segundos de espera depois de um lote, proporcionais ao peso dele.

    Um lote de peso 500 espera a pausa cheia (60 s); um de peso 100 espera um
    quinto disso. Assim a carga diária (lotes leves) não fica esperando à toa,
    e o ritmo nunca passa de 500 de peso por minuto.
    """
    peso_lote = n_municipios * peso_por_local(inicio, fim)
    return pausa_cheia * min(1.0, peso_lote / PESO_MAXIMO_POR_MINUTO)


class RespostaInvalidaError(ValueError):
    """A API respondeu com sucesso (HTTP 2xx), mas o conteúdo não são os dados esperados."""


def ler_json(resposta: requests.Response) -> Any:
    """Converte a resposta em JSON, explicando o que veio quando não é JSON.

    Já aconteceu de a API (ou algo no caminho, como um proxy) responder "200 OK" com
    uma página de aviso em vez dos dados. Sem este tratamento, o erro seria um
    "Expecting value" que não diz nada; com ele, a mensagem mostra o que chegou.
    """
    try:
        dados = resposta.json()
    except ValueError as erro:
        tipo = resposta.headers.get("Content-Type", "desconhecido")
        trecho = resposta.text[:300].strip() or "(resposta vazia)"
        raise RespostaInvalidaError(
            f"A API respondeu HTTP {resposta.status_code} com conteúdo que não é JSON "
            f"(Content-Type: {tipo}). Início da resposta: {trecho!r}"
        ) from erro

    # O Open-Meteo também avisa erros com um JSON {"error": true, "reason": "..."}.
    if isinstance(dados, dict) and dados.get("error"):
        raise RespostaInvalidaError(f"A API devolveu um erro: {dados.get('reason', dados)}")
    return dados


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
    dados = ler_json(resposta)

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


def _ultimas_datas_vazio() -> pd.DataFrame:
    return pd.DataFrame(
        {"codigo_ibge": pd.Series(dtype="int64"), "ultima_data": pd.Series(dtype="object")}
    )


def ultimas_datas_carregadas(config: ConfigLake, uf: str) -> pd.DataFrame:
    """Última data com dado válido de cada município da UF, lida do bronze.

    É o "checkpoint" da carga incremental. Leio só a UF pedida e só três colunas,
    então a consulta é rápida mesmo com o bronze grande.
    """
    if not existe(CAMINHO_CLIMA, config):
        return _ultimas_datas_vazio()

    bronze = ler_parquet(
        CAMINHO_CLIMA,
        config,
        filtros=[[("uf_sigla", "=", uf)]],
        colunas=["codigo_ibge", "data", VARIAVEL_REFERENCIA],
    )
    # Só conta como "feito" o dia que veio com valor. Um dia que veio nulo será
    # pedido de novo na próxima execução, quando a API já tiver o dado.
    validos = bronze[bronze[VARIAVEL_REFERENCIA].notna()]
    if validos.empty:
        return _ultimas_datas_vazio()

    ultimas = validos.groupby("codigo_ibge", as_index=False)["data"].max()
    ultimas = ultimas.rename(columns={"data": "ultima_data"})
    return ultimas.astype({"codigo_ibge": "int64"})


def calcular_pendencias(
    municipios: pd.DataFrame,
    ja_carregado: pd.DataFrame,
    inicio: date,
    fim: date,
) -> pd.DataFrame:
    """Diz, para cada município, a partir de que dia ainda falta buscar.

    Função pura. Devolve os municípios com a coluna "inicio_pendente" e só os
    que ainda têm algo a buscar até "fim".
    """
    df = municipios.merge(
        ja_carregado[["codigo_ibge", "ultima_data"]], on="codigo_ibge", how="left"
    )

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
    execucao: Execucao | None = None,
) -> int:
    """Busca e grava o clima que falta para os municípios de uma UF.

    Grava cada lote assim que ele chega, para que uma interrupção (limite da API,
    queda de rede) perca no máximo o lote em andamento.
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

    # Com --reprocessar eu ignoro o que já foi carregado e busco o período inteiro de
    # novo, útil para corrigir um intervalo. As versões repetidas que isso cria no
    # bronze são resolvidas pela silver, que fica com a gravada por último.
    ja_carregado = _ultimas_datas_vazio() if reprocessar else ultimas_datas_carregadas(config, uf)
    pendentes = calcular_pendencias(municipios_uf, ja_carregado, inicio, fim)
    if pendentes.empty:
        logger.info("%s: tudo em dia até %s. Nada a buscar.", uf, fim)
        return 0

    # Municípios com o mesmo dia de início podem ir juntos na mesma chamada. Em
    # geral é um grupo só; mais de um aparece quando uma carga anterior parou no meio.
    total = 0
    # Espera devida pelo lote anterior: o peso que ele gastou da cota do minuto.
    pausa_pendente = 0.0
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
            lote = grupo.iloc[posicao : posicao + tamanho]
            # Respeito o limite por minuto da API com uma pausa entre as chamadas,
            # proporcional ao peso do lote anterior.
            if pausa_pendente > 0:
                logger.info("Pausa de %.0f s para respeitar o limite da API.", pausa_pendente)
                time.sleep(pausa_pendente)

            extraido_em = datetime.now(UTC)
            respostas = extrair_clima(lote, inicio_grupo, fim, sessao)
            pausa_pendente = pausa_do_lote(len(lote), inicio_grupo, fim, pausa_segundos)
            # Conto a chamada logo depois de feita, para o log registrar até as
            # chamadas de um lote que falhar mais adiante.
            if execucao:
                execucao.chamadas_api += 1
            df = transformar_clima(respostas, lote, extraido_em)
            validar_clima(df, len(lote), inicio_grupo, fim)

            # Marco a hora da gravação logo antes de gravar. É ela (e não a hora da
            # extração, anterior à chamada à API) que a silver usa para saber o que
            # é novo: com cargas em paralelo, um lote extraído antes pode ser gravado
            # depois, e pela hora da extração ele ficaria de fora da silver.
            df["_gravado_em"] = pd.Timestamp(datetime.now(UTC))
            anexar_parquet(df, CAMINHO_CLIMA, config, PARTICOES, id_lote=uuid.uuid4().hex)

            total += len(df)
            if execucao:
                execucao.linhas_novas += len(df)
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
    limite_e_erro: bool = True,
) -> None:
    """Carrega o clima que falta para as UFs pedidas, uma UF por vez.

    Com limite_e_erro=False (a carga diária do Airflow), chegar ao limite da API é
    registrado como aviso, e não como erro: ali isso é o esperado.
    """
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

    parametros = {
        "ufs": ufs,
        "inicio": inicio,
        "fim": fim,
        "limite": limite,
        "reprocessar": reprocessar,
    }
    # Tudo o que acontece dentro do "with" fica registrado no log de execuções,
    # inclusive quando a carga termina em erro ou no limite da API.
    with registrar_carga("clima", parametros, config) as execucao:
        for uf in ufs:
            municipios_uf = municipios[municipios["uf_sigla"].astype(str) == uf]
            municipios_uf = municipios_uf.sort_values("codigo_ibge")
            if limite:
                # Modo de teste: só as primeiras N cidades da UF.
                municipios_uf = municipios_uf.head(limite)

            try:
                linhas = carregar_clima_uf(
                    municipios_uf,
                    inicio,
                    fim,
                    sessao,
                    config,
                    reprocessar=reprocessar,
                    execucao=execucao,
                )
            except requests.exceptions.RetryError:
                # As tentativas se esgotaram, o que costuma ser o limite diário da
                # API. Os lotes já gravados estão no bronze: é só rodar de novo
                # depois, que a carga continua de onde parou.
                nivel = logging.ERROR if limite_e_erro else logging.WARNING
                logger.log(
                    nivel,
                    "%s: limite da API atingido. O progresso foi salvo; rode de novo "
                    "mais tarde que a carga continua de onde parou.",
                    uf,
                )
                raise
            logger.info("%s concluída: %d linha(s) nova(s) em %s.", uf, linhas, CAMINHO_CLIMA)


def ordenar_ufs(uf_de_cada_municipio: list[str]) -> list[str]:
    """Ordena as UFs da que tem menos municípios para a que tem mais.

    Recebe a UF de cada município (com repetição) e conta quantos cada uma tem.
    UFs pequenas primeiro fecham mais estados por execução: o painel passa a
    mostrar UFs completas mais cedo, em vez de uma UF grande pela metade. Em caso
    de empate, a ordem alfabética decide.

    A ordem fixa também importa porque cada execução para no limite diário da
    API: uma UF começada termina antes de a próxima começar.
    """
    contagem = Counter(uf.upper() for uf in uf_de_cada_municipio)
    return sorted(contagem, key=lambda uf: (contagem[uf], uf))


# UF de referência do histórico: as demais são carregadas até o último dia que ela
# tem completo antes de qualquer UF receber dias novos.
UF_REFERENCIA = "SP"


def data_de_corte(config: ConfigLake, uf: str = UF_REFERENCIA) -> date | None:
    """Último dia que todos os municípios já carregados da UF de referência têm.

    É o mínimo das últimas datas dos municípios da UF. Sem dado nenhum da UF,
    devolve None (não há corte e a carga segue direto até o fim).
    """
    ultimas = ultimas_datas_carregadas(config, uf)
    if ultimas.empty:
        return None
    return pd.Timestamp(ultimas["ultima_data"].min()).date()


def carregar_clima_brasil(
    fim: date,
    config: ConfigLake | None = None,
    sessao: requests.Session | None = None,
) -> str:
    """Carrega o clima que falta para o Brasil inteiro, até onde a cota diária da API deixar.

    É a carga que o Airflow roda todo dia, em duas fases:
    1. fechar o histórico: todas as UFs até o último dia que a UF de referência
       (SP) tem completo, sem buscar dia novo para ninguém. Assim a cota vai
       primeiro para quem está atrasado, e não para os dias novos de SP;
    2. avançar: só se a fase 1 terminar inteira (todas as UFs alcançaram o corte),
       busca os dias novos, até `fim`, para todo o Brasil.

    Chegar ao limite da API não é erro aqui: o que foi gravado fica no bronze e a
    próxima execução continua de onde parou. Devolve "sucesso" ou "limite_api".
    """
    config = config or carregar_config()
    municipios = ler_parquet(CAMINHO_MUNICIPIOS, config, colunas=["uf_sigla"])
    ufs = ordenar_ufs(municipios["uf_sigla"].astype(str).tolist())

    corte = data_de_corte(config)
    fases = []
    if corte is not None and corte < fim:
        fases.append(("fechar o histórico", corte))
    fases.append(("avançar", fim))

    try:
        for nome, ate in fases:
            logger.info(
                "Fase '%s': todas as UFs até %s, nesta ordem: %s", nome, ate, ", ".join(ufs)
            )
            carregar_clima(ufs, DATA_INICIO, ate, config=config, sessao=sessao, limite_e_erro=False)
    except requests.exceptions.RetryError:
        logger.warning("Cota diária da API esgotada. O restante fica para a próxima execução.")
        return STATUS_LIMITE_API
    return STATUS_SUCESSO


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
        help="Ignora o que já foi carregado e busca o período inteiro de novo",
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
