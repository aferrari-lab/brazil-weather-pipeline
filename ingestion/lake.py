"""Acesso ao data lake (RustFS, compatível com S3): gravar e ler Parquet.

Todos os extratores gravam por aqui, então a forma de falar com o lake fica em
um lugar só. Para trocar o RustFS pela AWS S3 no futuro, basta mudar o .env.
"""

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import fsspec
import pandas as pd
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Endpoint padrão para quem roda o código no próprio PC. Dentro dos containers do
# Airflow, o docker-compose sobrescreve esta variável com o endereço interno.
ENDPOINT_PADRAO = "http://localhost:9000"


@dataclass(frozen=True)
class ConfigLake:
    """Onde fica o lake e como acessá-lo.

    raiz: começo de todos os caminhos, ex.: "s3://lake" (ou uma pasta local nos testes).
    storage_options: credenciais e endpoint que o pandas repassa ao s3fs.
    """

    raiz: str
    storage_options: dict[str, Any] | None = field(default=None, repr=False)


def carregar_config() -> ConfigLake:
    """Monta a configuração do lake a partir das variáveis de ambiente (.env)."""
    # O load_dotenv lê o .env da pasta atual, mas não sobrescreve variáveis que
    # já existem no ambiente. Assim, no Airflow, valem as variáveis do container.
    load_dotenv()

    # Falho cedo, com uma mensagem clara, se faltar alguma variável obrigatória.
    # Nunca coloco senha no código: tudo vem do .env, que não vai para o Git.
    obrigatorias = ["LAKE_ACCESS_KEY", "LAKE_SECRET_KEY", "LAKE_BUCKET"]
    faltando = [nome for nome in obrigatorias if not os.getenv(nome)]
    if faltando:
        raise OSError(f"Variáveis de ambiente do lake não definidas: {faltando}. Confira o .env.")

    endpoint = os.getenv("S3_ENDPOINT_URL", ENDPOINT_PADRAO)

    # Estas opções vão para o s3fs, a biblioteca que o pandas usa para falar S3.
    # O addressing_style "path" monta URLs como http://servidor/bucket/arquivo.
    # Servidores S3 locais, como o RustFS, funcionam assim; o estilo padrão da AWS
    # (http://bucket.servidor/arquivo) não resolveria no localhost.
    storage_options = {
        "key": os.environ["LAKE_ACCESS_KEY"],
        "secret": os.environ["LAKE_SECRET_KEY"],
        "client_kwargs": {"endpoint_url": endpoint, "region_name": "us-east-1"},
        "config_kwargs": {"s3": {"addressing_style": "path"}},
    }

    logger.info("Lake configurado: bucket '%s' em %s", os.environ["LAKE_BUCKET"], endpoint)
    return ConfigLake(raiz=f"s3://{os.environ['LAKE_BUCKET']}", storage_options=storage_options)


def caminho_completo(caminho: str, config: ConfigLake) -> str:
    """Junta a raiz do lake com um caminho relativo, ex.: 'bronze/municipios'."""
    # Exijo caminhos relativos e sem "..", para nenhum extrator gravar fora do
    # lugar dele por engano.
    caminho = caminho.strip("/")
    if not caminho or ".." in caminho.split("/"):
        raise ValueError(f"Caminho inválido no lake: '{caminho}'")
    return f"{config.raiz.rstrip('/')}/{caminho}"


def gravar_parquet(
    df: pd.DataFrame,
    caminho: str,
    config: ConfigLake | None = None,
    particoes: list[str] | None = None,
) -> str:
    """Grava um DataFrame em Parquet no lake e devolve o caminho completo.

    Sem partições: grava um único arquivo e sobrescreve o anterior.
    Com partições: grava uma pasta por valor (ex.: ano=2026/mes=10/uf=SP) e
    substitui só as partições presentes no DataFrame, sem tocar nas outras.
    """
    config = config or carregar_config()
    destino = caminho_completo(caminho, config)

    if df.empty:
        raise ValueError(f"Tentativa de gravar um DataFrame vazio em {destino}.")

    # Em pasta local (usada nos testes), o pandas não cria as pastas sozinho.
    # No S3 não existem pastas de verdade, então lá não há nada a criar.
    if not destino.startswith("s3://"):
        alvo = Path(destino) if particoes else Path(destino).parent
        alvo.mkdir(parents=True, exist_ok=True)

    if particoes:
        faltando = set(particoes) - set(df.columns)
        if faltando:
            raise ValueError(f"Colunas de partição ausentes no DataFrame: {sorted(faltando)}")

        # O existing_data_behavior="delete_matching" é o que deixa a gravação
        # idempotente: antes de escrever uma partição, o pyarrow apaga o que já
        # existia nela. Sem isso, rodar a carga duas vezes duplicaria as linhas.
        df.to_parquet(
            destino,
            index=False,
            partition_cols=particoes,
            storage_options=config.storage_options,
            existing_data_behavior="delete_matching",
        )
    else:
        # Arquivo único: gravar no mesmo caminho já substitui o anterior.
        df.to_parquet(destino, index=False, storage_options=config.storage_options)

    logger.info("Gravadas %d linhas em %s", len(df), destino)
    return destino


def existe(caminho: str, config: ConfigLake | None = None) -> bool:
    """Diz se um arquivo ou pasta existe no lake."""
    config = config or carregar_config()
    destino = caminho_completo(caminho, config)
    # O fsspec é a camada que o pandas usa por baixo: o mesmo código serve para
    # o S3 e para uma pasta local (nos testes).
    sistema, caminho_interno = fsspec.core.url_to_fs(destino, **(config.storage_options or {}))
    return sistema.exists(caminho_interno)


def ler_parquet(
    caminho: str,
    config: ConfigLake | None = None,
    filtros: list[list[tuple[str, str, Any]]] | None = None,
    schema: Any = None,
    colunas: list[str] | None = None,
) -> pd.DataFrame:
    """Lê um arquivo ou uma pasta particionada de Parquet do lake.

    colunas: lê só as colunas pedidas. O Parquet guarda cada coluna separada, então
    ler três colunas de uma tabela de quinze é bem mais rápido do que ler tudo.

    schema: schema do pyarrow para ler uma pasta com vários arquivos de forma
    consistente, mesmo que algum arquivo tenha gravado uma coluna toda vazia.

    filtros: lê só as partições pedidas, no formato do pyarrow. Uma lista de
    condições "E" dentro de uma lista "OU", ex.: [[("ano", "=", 2025), ("mes", "=", 1)]].
    """
    config = config or carregar_config()
    origem = caminho_completo(caminho, config)

    # Com filtros, o pyarrow nem abre as pastas que não interessam. Isso importa
    # quando o lake tiver centenas de partições e eu só quiser algumas.
    df = pd.read_parquet(
        origem,
        storage_options=config.storage_options,
        filters=filtros,
        schema=schema,
        columns=colunas,
    )
    logger.info("Lidas %d linhas de %s", len(df), origem)
    return df


def anexar_parquet(
    df: pd.DataFrame,
    caminho: str,
    config: ConfigLake | None,
    particoes: list[str],
    id_lote: str,
) -> str:
    """Acrescenta um lote ao lake como arquivos novos, sem tocar no que já existe.

    É a gravação "append-only" do bronze: cada lote vira um arquivo novo dentro de
    cada partição (ex.: ano=2025/mes=1/uf_sigla=SP/<id_lote>-0.parquet).
    - Nada é apagado nem reescrito, então uma queda no meio da gravação nunca
      estraga dados que já estavam lá.
    - O custo de gravar um lote não cresce com o tamanho do lake.
    - Duas cargas gravando ao mesmo tempo não interferem uma na outra.
    Se o mesmo dado for gravado duas vezes (um lote refeito, por exemplo), as
    duas versões ficam no bronze, e a silver fica com a mais recente.
    """
    config = config or carregar_config()
    destino = caminho_completo(caminho, config)

    if df.empty:
        raise ValueError(f"Tentativa de anexar um DataFrame vazio em {destino}.")
    faltando = set(particoes) - set(df.columns)
    if faltando:
        raise ValueError(f"Colunas de partição ausentes no DataFrame: {sorted(faltando)}")

    # O id do lote no nome do arquivo garante que ele seja novo, e o
    # "overwrite_or_ignore" diz ao pyarrow para não apagar nada que já exista.
    df.to_parquet(
        destino,
        index=False,
        partition_cols=particoes,
        storage_options=config.storage_options,
        basename_template=f"{id_lote}-{{i}}.parquet",
        existing_data_behavior="overwrite_or_ignore",
    )
    logger.info("Anexadas %d linhas em %s (lote %s)", len(df), destino, id_lote)
    return destino
