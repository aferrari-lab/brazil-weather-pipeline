"""Tabela de municípios do Brasil: cadastro do IBGE + coordenadas das sedes.

Executar (com o .venv ativo, na raiz do projeto):
    python -m ingestion.municipios
"""

import io
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Uso logging em vez de print: cada mensagem sai com horário e nível (INFO,
# WARNING, ERROR), e o Airflow captura esses logs quando o código rodar numa DAG.
logger = logging.getLogger(__name__)

# Deixo as constantes no topo do módulo para ficarem fáceis de achar e de trocar,
# sem textos "mágicos" espalhados pelo código.
URL_IBGE_MUNICIPIOS = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
TIMEOUT_SEGUNDOS = 30
FONTE_IBGE = "ibge_localidades_v1"


# Eu me identifico para a API com um User-Agent próprio. É uma cortesia com quem
# mantém o serviço e facilita o contato caso o meu uso cause algum problema.
USER_AGENT = (
    "brazil-weather-pipeline/0.1 (+https://github.com/aferrari-lab/brazil-weather-pipeline)"
)

# Aqui eu mapeio cada "caminho no JSON achatado" para o nome final da coluna.
# O pd.json_normalize transforma o JSON aninhado em colunas cujos nomes são o
# caminho das chaves separado por ponto. Aqui eu escolho somente o que interessa.
# Uso o caminho por "regiao-imediata", e não por "microrregiao", porque há
# municípios recentes com "microrregiao" nulo, o que quebraria a extração.
COLUNAS_IBGE = {
    "id": "codigo_ibge",
    "nome": "nome",
    "regiao-imediata.regiao-intermediaria.UF.sigla": "uf_sigla",
    "regiao-imediata.regiao-intermediaria.UF.regiao.nome": "regiao",
}

# Declaro o schema de saída explicitamente. Assim o tipo de cada coluna não
# depende do que o pandas "adivinhar", e quem lê o código sabe o que esperar.
SCHEMA_SAIDA = {
    "codigo_ibge": "int64",
    "nome": "string",
    "uf_sigla": "string",
    "regiao": "string",
}

# --- Coordenadas das cidades (sedes municipais) ---
# A API do IBGE não traz latitude e longitude, então eu busco as coordenadas das
# sedes neste CSV público, que usa o mesmo código IBGE como chave.
URL_COORDENADAS = (
    "https://raw.githubusercontent.com/kelvins/municipios-brasileiros/main/csv/municipios.csv"
)
FONTE_COORDENADAS = "kelvins_municipios_brasileiros"

SCHEMA_COORDENADAS = {
    "codigo_ibge": "int64",
    "latitude": "float64",
    "longitude": "float64",
}

# Limites aproximados do território brasileiro, com uma pequena folga. Uso para
# pegar coordenadas erradas, como latitude e longitude trocadas ou sinal invertido.
LATITUDE_MIN, LATITUDE_MAX = -34.0, 6.0
LONGITUDE_MIN, LONGITUDE_MAX = -74.0, -28.0

# --- Tabela final de municípios ---
# A tabela final junta as duas fontes, então registro ambas na coluna _fonte.
FONTE_MUNICIPIOS = f"{FONTE_IBGE}+{FONTE_COORDENADAS}"

# Ordem fixa das colunas da tabela final, para o arquivo sair sempre igual.
COLUNAS_MUNICIPIOS = [
    "codigo_ibge",
    "nome",
    "uf_sigla",
    "regiao",
    "latitude",
    "longitude",
    "_fonte",
    "_extraido_em",
]

# Por enquanto eu salvo localmente, na pasta data/ (fora do Git). No passo 2 da
# Fase 2, este destino passa a ser o data lake.
CAMINHO_MUNICIPIOS = Path("data/municipios.parquet")


def criar_sessao(tentativas: int = 3, backoff: float = 1.0) -> requests.Session:
    """Cria uma sessão HTTP com retry automático e backoff exponencial.

    Repete a requisição em falhas temporárias (limite de taxa e erros 5xx),
    esperando cada vez mais entre as tentativas.
    """
    # Só repito erros que costumam ser passageiros: 429 (limite de requisições)
    # e falhas do servidor. Um 404, por exemplo, não melhora tentando de novo.
    # Com backoff=1, as esperas entre tentativas crescem de forma exponencial,
    # e se a API mandar o cabeçalho Retry-After, eu respeito o tempo que ela pede.
    retry = Retry(
        total=tentativas,
        backoff_factor=backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
    )
    # Uso uma Session para reaproveitar a conexão entre chamadas, o que vai fazer
    # diferença quando eu chamar o Open-Meteo milhares de vezes.
    sessao = requests.Session()
    sessao.mount("https://", HTTPAdapter(max_retries=retry))
    sessao.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
    return sessao


def extrair_municipios_ibge(sessao: requests.Session | None = None) -> list[dict[str, Any]]:
    """Baixa o cadastro bruto de municípios da API de localidades do IBGE."""
    # Recebo a sessão como parâmetro para poder reaproveitá-la entre extratores
    # e, nos testes, trocá-la por uma falsa. Se ninguém passar, eu crio uma.
    sessao = sessao or criar_sessao()

    logger.info("Baixando municípios do IBGE: %s", URL_IBGE_MUNICIPIOS)

    # O timeout continua necessário mesmo com retry: a sessão não tem timeout
    # padrão, e sem ele uma chamada travada deixaria o pipeline parado para sempre.
    resposta = sessao.get(URL_IBGE_MUNICIPIOS, timeout=TIMEOUT_SEGUNDOS)

    # Transformo qualquer erro HTTP que sobrou depois das tentativas em exceção.
    # Sem isso, eu seguiria tratando uma página de erro como se fossem dados.
    resposta.raise_for_status()

    dados = resposta.json()

    # Uma resposta "200 OK" vazia também é falha. Prefiro parar aqui a seguir
    # com zero municípios e só descobrir o problema lá na frente.
    if not isinstance(dados, list) or not dados:
        raise ValueError("A API do IBGE devolveu uma resposta vazia ou em formato inesperado.")

    logger.info("Recebidos %d municípios do IBGE", len(dados))
    return dados


def transformar_municipios_ibge(dados: list[dict[str, Any]], extraido_em: datetime) -> pd.DataFrame:
    """Converte o JSON do IBGE em uma tabela plana com schema fixo.

    Função pura (sem rede nem disco): recebe os dados e devolve a tabela,
    o que permite testá-la com um JSON de exemplo.
    """
    # Achato o JSON: cada chave aninhada vira uma coluna,
    # ex.: "regiao-imediata.regiao-intermediaria.UF.sigla".
    df = pd.json_normalize(dados)

    # Se o IBGE mudar o formato da resposta, prefiro falhar logo aqui, com uma
    # mensagem clara, a gravar dados errados no lake.
    faltando = set(COLUNAS_IBGE) - set(df.columns)
    if faltando:
        raise ValueError(f"A resposta do IBGE não tem as colunas esperadas: {sorted(faltando)}")

    # Mantenho só as colunas do mapa, já as renomeio e aplico o schema.
    # O tipo explícito importa porque o código IBGE será a chave do merge com as
    # coordenadas, e chaves de tipos diferentes (int x texto) não casam, sem dar erro.
    df = df[list(COLUNAS_IBGE)].rename(columns=COLUNAS_IBGE).astype(SCHEMA_SAIDA)

    # Adiciono colunas de metadados para rastrear a origem de cada linha: de qual
    # fonte veio e quando foi extraída. O prefixo "_" separa o que é controle do
    # pipeline do que é dado de negócio.
    df["_fonte"] = FONTE_IBGE
    df["_extraido_em"] = pd.Timestamp(extraido_em)

    return df


def validar_municipios_ibge(df: pd.DataFrame) -> None:
    """Confere a qualidade da tabela. Erros críticos interrompem; o resto vira aviso."""
    # Problemas que tornam a tabela inutilizável: eu interrompo o pipeline.
    if df.empty:
        raise ValueError("A tabela de municípios está vazia.")
    if not df["codigo_ibge"].is_unique:
        duplicados = df.loc[df["codigo_ibge"].duplicated(), "codigo_ibge"].tolist()
        raise ValueError(f"Há códigos IBGE duplicados: {duplicados[:10]}")

    # Problemas pontuais: eu registro um aviso com os casos, mas não descarto
    # nada em silêncio. A decisão sobre o que fazer com eles fica explícita.
    nulos = df[list(SCHEMA_SAIDA)].isna().sum()
    for coluna, quantidade in nulos[nulos > 0].items():
        exemplos = df.loc[df[coluna].isna(), "nome"].head(5).tolist()
        logger.warning("%d município(s) sem '%s'. Exemplos: %s", quantidade, coluna, exemplos)

    logger.info("Validação concluída: %d municípios, %d UFs.", len(df), df["uf_sigla"].nunique())


def buscar_municipios_ibge(sessao: requests.Session | None = None) -> pd.DataFrame:
    """Executa extração, transformação e validação do cadastro do IBGE."""
    # Registro o horário uma única vez, no início, para que todas as linhas desta
    # carga tenham exatamente o mesmo carimbo de extração. Gravo em UTC, o padrão
    # para armazenamento: não depende do fuso da máquina que roda o pipeline nem
    # de horário de verão. A conversão para o horário de Brasília fica para a
    # hora de exibir os dados.
    extraido_em = datetime.now(UTC)

    dados = extrair_municipios_ibge(sessao)
    df = transformar_municipios_ibge(dados, extraido_em)
    validar_municipios_ibge(df)
    return df


def extrair_coordenadas(sessao: requests.Session | None = None) -> str:
    """Baixa o CSV com as coordenadas das sedes municipais e devolve o texto bruto."""
    # O pd.read_csv até consegue ler direto da URL, mas assim eu perderia o retry,
    # o timeout e o User-Agent da minha sessão. Por isso eu baixo com a sessão e
    # só depois transformo o texto em tabela.
    sessao = sessao or criar_sessao()

    logger.info("Baixando coordenadas das cidades: %s", URL_COORDENADAS)
    resposta = sessao.get(URL_COORDENADAS, timeout=TIMEOUT_SEGUNDOS)
    resposta.raise_for_status()

    # Mesmo com "200 OK", um arquivo vazio é falha: prefiro parar aqui.
    csv_texto = resposta.text
    if not csv_texto.strip():
        raise ValueError("O CSV de coordenadas veio vazio.")

    logger.info("CSV de coordenadas recebido (%d bytes)", len(csv_texto))
    return csv_texto


def transformar_coordenadas(csv_texto: str, extraido_em: datetime) -> pd.DataFrame:
    """Converte o CSV de coordenadas em uma tabela com schema fixo.

    Função pura (sem rede nem disco), testável com um CSV de exemplo.
    """
    # O io.StringIO faz o texto se comportar como um arquivo aberto, que é o que
    # o pd.read_csv espera receber.
    df = pd.read_csv(io.StringIO(csv_texto))

    # Se o formato do CSV mudar, eu quebro aqui com uma mensagem clara.
    faltando = set(SCHEMA_COORDENADAS) - set(df.columns)
    if faltando:
        raise ValueError(f"O CSV de coordenadas não tem as colunas esperadas: {sorted(faltando)}")

    # Mantenho só as colunas que me interessam e aplico o schema. O codigo_ibge
    # precisa ser int64, igual ao da tabela do IBGE, para o merge casar.
    df = df[list(SCHEMA_COORDENADAS)].astype(SCHEMA_COORDENADAS)

    df["_fonte"] = FONTE_COORDENADAS
    df["_extraido_em"] = pd.Timestamp(extraido_em)
    return df


def validar_coordenadas(df: pd.DataFrame) -> None:
    """Confere a qualidade das coordenadas. Erros críticos interrompem; o resto vira aviso."""
    if df.empty:
        raise ValueError("A tabela de coordenadas está vazia.")
    if not df["codigo_ibge"].is_unique:
        duplicados = df.loc[df["codigo_ibge"].duplicated(), "codigo_ibge"].tolist()
        raise ValueError(f"Há códigos IBGE duplicados nas coordenadas: {duplicados[:10]}")

    # Coordenada fora do Brasil é crítica: com ela, eu buscaria no Open-Meteo o
    # clima de outro lugar sem perceber. O ~ inverte o resultado do between, então
    # "fora" é tudo o que não está entre os limites. Nulos ficam de fora desta
    # checagem (notna) porque são tratados como aviso logo abaixo.
    com_coordenada = df["latitude"].notna() & df["longitude"].notna()
    fora_do_brasil = com_coordenada & (
        ~df["latitude"].between(LATITUDE_MIN, LATITUDE_MAX)
        | ~df["longitude"].between(LONGITUDE_MIN, LONGITUDE_MAX)
    )
    if fora_do_brasil.any():
        exemplos = df.loc[fora_do_brasil, "codigo_ibge"].head(10).tolist()
        raise ValueError(
            f"{int(fora_do_brasil.sum())} coordenada(s) fora do Brasil. Códigos: {exemplos}"
        )

    # Coordenada faltando: eu aviso com exemplos, sem descartar em silêncio.
    sem_coordenada = ~com_coordenada
    if sem_coordenada.any():
        exemplos = df.loc[sem_coordenada, "codigo_ibge"].head(5).tolist()
        logger.warning(
            "%d cidade(s) sem coordenada. Códigos: %s", int(sem_coordenada.sum()), exemplos
        )

    logger.info("Validação das coordenadas concluída: %d cidades.", len(df))


def buscar_coordenadas(sessao: requests.Session | None = None) -> pd.DataFrame:
    """Executa extração, transformação e validação das coordenadas das cidades."""
    # Mesmo padrão da carga do IBGE: um único carimbo de horário, em UTC.
    extraido_em = datetime.now(UTC)

    csv_texto = extrair_coordenadas(sessao)
    df = transformar_coordenadas(csv_texto, extraido_em)
    validar_coordenadas(df)
    return df


def combinar_municipios(municipios: pd.DataFrame, coordenadas: pd.DataFrame) -> pd.DataFrame:
    """Junta o cadastro do IBGE com as coordenadas pelo código IBGE.

    Função pura: mantém todos os municípios do IBGE, mesmo os sem coordenada,
    e descarta coordenadas de códigos que não existem no IBGE (com aviso).
    """
    # O IBGE é a fonte oficial do cadastro, então ele manda: uso how="left" para
    # manter todos os municípios dele, tenham ou não coordenada.
    # O validate="one_to_one" faz o pandas levantar erro se algum código aparecer
    # repetido em um dos lados, o que multiplicaria linhas sem ninguém perceber.
    # O indicator=True cria a coluna "_merge", que diz de qual lado veio cada linha.
    combinado = municipios.drop(columns=["_fonte", "_extraido_em"]).merge(
        coordenadas[["codigo_ibge", "latitude", "longitude"]],
        on="codigo_ibge",
        how="outer",
        validate="one_to_one",
        indicator=True,
    )

    # Uso how="outer" no merge (e não "left") só para conseguir enxergar o caso
    # contrário: coordenadas cujo código não existe no IBGE. Eu registro um aviso
    # com esses códigos e depois as descarto, porque o IBGE é a referência.
    so_coordenada = combinado["_merge"] == "right_only"
    if so_coordenada.any():
        logger.warning(
            "%d coordenada(s) sem município correspondente no IBGE (descartadas). Códigos: %s",
            int(so_coordenada.sum()),
            combinado.loc[so_coordenada, "codigo_ibge"].head(10).tolist(),
        )

    # Municípios sem coordenada continuam na tabela, mas eu aviso com os nomes.
    # Não dá para buscar clima sem coordenada, então decido o que fazer com eles
    # de forma explícita mais adiante, em vez de sumir com eles aqui.
    sem_coordenada = combinado["_merge"] == "left_only"
    if sem_coordenada.any():
        exemplos = (
            combinado.loc[sem_coordenada, ["nome", "uf_sigla"]]
            .astype(str)
            .agg("/".join, axis=1)
            .head(10)
            .tolist()
        )
        logger.warning(
            "%d município(s) do IBGE sem coordenada. Exemplos: %s",
            int(sem_coordenada.sum()),
            exemplos,
        )

    combinado = combinado.loc[~so_coordenada].drop(columns="_merge")

    # Carimbo de extração: uso o da carga do IBGE, que é a tabela de referência.
    combinado["_fonte"] = FONTE_MUNICIPIOS
    combinado["_extraido_em"] = municipios["_extraido_em"].iloc[0]

    # Ordeno pelo código para o arquivo sair sempre na mesma ordem e reseto o
    # índice, que ficou bagunçado depois do merge e do filtro.
    return (
        combinado[COLUNAS_MUNICIPIOS]
        .astype({"codigo_ibge": "int64"})
        .sort_values("codigo_ibge")
        .reset_index(drop=True)
    )


def validar_municipios(df: pd.DataFrame) -> None:
    """Confere a tabela final antes de salvar."""
    if df.empty:
        raise ValueError("A tabela final de municípios está vazia.")
    if not df["codigo_ibge"].is_unique:
        raise ValueError("A tabela final tem códigos IBGE duplicados.")

    # Resumo da cobertura, que é o número que mais importa para a próxima etapa:
    # só municípios com coordenada terão clima.
    com_coordenada = int((df["latitude"].notna() & df["longitude"].notna()).sum())
    cobertura = com_coordenada / len(df)
    logger.info(
        "Tabela final: %d municípios, %d com coordenada (%.2f%%).",
        len(df),
        com_coordenada,
        cobertura * 100,
    )

    # Se a cobertura cair muito, algo deu errado em uma das fontes (ex.: o CSV veio
    # truncado). Prefiro interromper a seguir com metade do Brasil sem clima.
    if cobertura < 0.99:
        raise ValueError(f"Cobertura de coordenadas baixa demais: {cobertura:.2%}.")


def montar_municipios(sessao: requests.Session | None = None) -> pd.DataFrame:
    """Busca as duas fontes, junta e valida a tabela final de municípios."""
    # Uso uma única sessão para as duas fontes, reaproveitando a conexão.
    sessao = sessao or criar_sessao()

    municipios = buscar_municipios_ibge(sessao)
    coordenadas = buscar_coordenadas(sessao)

    df = combinar_municipios(municipios, coordenadas)
    validar_municipios(df)
    return df


def salvar_municipios(df: pd.DataFrame, caminho: Path = CAMINHO_MUNICIPIOS) -> Path:
    """Grava a tabela de municípios em Parquet e devolve o caminho do arquivo."""
    # Crio a pasta se ela ainda não existir. O parents=True cria também as pastas
    # intermediárias, e o exist_ok=True evita erro se ela já existir.
    caminho.parent.mkdir(parents=True, exist_ok=True)

    # Gravar sempre o arquivo inteiro, sobrescrevendo, deixa a carga idempotente:
    # rodar duas vezes dá o mesmo resultado, sem duplicar linhas.
    df.to_parquet(caminho, index=False)

    logger.info("Tabela de municípios salva em %s (%d linhas).", caminho, len(df))
    return caminho


if __name__ == "__main__":
    # Configuro o formato dos logs só quando o arquivo roda direto. Quando ele for
    # importado por uma DAG, quem decide o formato é o Airflow.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    # Monto a tabela final (IBGE + coordenadas), valido e salvo em Parquet.
    municipios = montar_municipios()
    salvar_municipios(municipios)
    logger.info("Amostra:\n%s", municipios.head())
