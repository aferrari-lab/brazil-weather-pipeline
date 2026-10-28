"""Acesso ao data lake (RustFS, compatível com S3): gravar e ler Parquet.

Todos os extratores gravam por aqui, então a forma de falar com o lake fica em
um lugar só. Para trocar o RustFS pela AWS S3 no futuro, basta mudar o .env.
"""

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

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


def ler_parquet(caminho: str, config: ConfigLake | None = None) -> pd.DataFrame:
    """Lê um arquivo ou uma pasta particionada de Parquet do lake."""
    config = config or carregar_config()
    origem = caminho_completo(caminho, config)

    df = pd.read_parquet(origem, storage_options=config.storage_options)
    logger.info("Lidas %d linhas de %s", len(df), origem)
    return df
