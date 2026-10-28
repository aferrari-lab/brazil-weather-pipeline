"""Log de execuções das cargas: uma linha por execução, gravada no lake.

Cada carga registra quando começou e terminou, o que processou, quantas linhas
trouxe, quantas chamadas fez à API e como terminou (sucesso, limite da API ou
erro). É a base do painel de saúde do pipeline (Fase 6).

Uso:
    with registrar_carga("clima", {"ufs": ["SP"]}, config) as execucao:
        ...
        execucao.chamadas_api += 1
        execucao.linhas_novas += len(df)
"""

import json
import logging
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import pandas as pd
import pyarrow as pa
import requests

from ingestion.lake import ConfigLake, carregar_config, existe, gravar_parquet, ler_parquet

logger = logging.getLogger(__name__)

# Cada execução grava o seu próprio arquivo, em vez de todas editarem um arquivo
# único. Assim duas cargas rodando ao mesmo tempo nunca sobrescrevem o log uma da
# outra, e ler a pasta inteira devolve o histórico completo.
CAMINHO_LOG = "controle/log_cargas"

STATUS_SUCESSO = "sucesso"
STATUS_LIMITE_API = "limite_api"
STATUS_ERRO = "erro"
STATUS_INTERROMPIDA = "interrompida"

# Limite de tamanho da mensagem de erro guardada, para o log não crescer demais.
TAMANHO_MAXIMO_MENSAGEM = 500

# Schema fixo do log. Sem ele, uma execução de sucesso grava "mensagem_erro" toda
# vazia e o Parquet registra o tipo da coluna como "nulo"; depois, ao ler a pasta
# junto com uma execução que teve erro (texto), o pyarrow não consegue juntar os
# dois tipos. Com o schema declarado, todos os arquivos são lidos do mesmo jeito,
# inclusive os gravados antes desta correção.
SCHEMA_LOG = pa.schema(
    [
        ("id_execucao", pa.string()),
        ("processo", pa.string()),
        ("status", pa.string()),
        ("inicio", pa.timestamp("us", tz="UTC")),
        ("fim", pa.timestamp("us", tz="UTC")),
        ("duracao_segundos", pa.float64()),
        ("linhas_novas", pa.int64()),
        ("chamadas_api", pa.int64()),
        ("parametros", pa.string()),
        ("mensagem_erro", pa.string()),
    ]
)


@dataclass
class Execucao:
    """O que acontece durante uma execução. O código da carga atualiza os contadores."""

    processo: str
    parametros: dict[str, Any]
    id_execucao: str = field(default_factory=lambda: uuid.uuid4().hex)
    inicio: datetime = field(default_factory=lambda: datetime.now(UTC))
    linhas_novas: int = 0
    chamadas_api: int = 0


def classificar_erro(erro: BaseException | None) -> str:
    """Traduz como a execução terminou em um status do log."""
    if erro is None:
        return STATUS_SUCESSO
    # As tentativas do retry se esgotaram: quase sempre é o limite da API. Separo
    # esse caso porque ele não é um defeito: é só rodar de novo mais tarde.
    if isinstance(erro, requests.exceptions.RetryError):
        return STATUS_LIMITE_API
    # Ctrl + C (ou o Airflow cancelando a tarefa) também não é defeito do pipeline:
    # alguém parou a execução de propósito.
    if isinstance(erro, KeyboardInterrupt):
        return STATUS_INTERROMPIDA
    return STATUS_ERRO


def montar_registro(execucao: Execucao, erro: BaseException | None, fim: datetime) -> pd.DataFrame:
    """Monta a linha do log de uma execução. Função pura, testável."""
    mensagem = None
    if erro is not None:
        # Algumas exceções não têm texto (ex.: Ctrl + C); aí guardo só o nome.
        texto = str(erro)
        mensagem = f"{type(erro).__name__}: {texto}" if texto else type(erro).__name__
        mensagem = mensagem[:TAMANHO_MAXIMO_MENSAGEM]

    return pd.DataFrame(
        [
            {
                "id_execucao": execucao.id_execucao,
                "processo": execucao.processo,
                "status": classificar_erro(erro),
                "inicio": pd.Timestamp(execucao.inicio),
                "fim": pd.Timestamp(fim),
                "duracao_segundos": round((fim - execucao.inicio).total_seconds(), 1),
                "linhas_novas": execucao.linhas_novas,
                "chamadas_api": execucao.chamadas_api,
                # Guardo os parâmetros como JSON, para o log aceitar qualquer processo
                # sem precisar de uma coluna nova para cada parâmetro diferente.
                "parametros": json.dumps(execucao.parametros, default=str, ensure_ascii=False),
                "mensagem_erro": mensagem,
            }
        ]
    ).astype({"mensagem_erro": "string", "parametros": "string"})
    # O astype para "string" garante que a coluna seja gravada como texto mesmo
    # quando estiver vazia (execução sem erro).


@contextmanager
def registrar_carga(
    processo: str,
    parametros: dict[str, Any],
    config: ConfigLake,
) -> Iterator[Execucao]:
    """Registra uma execução no log, termine ela bem ou mal.

    Uso um context manager (o bloco "with") porque ele garante que o registro
    é gravado mesmo quando a carga levanta uma exceção no meio do caminho.
    """
    execucao = Execucao(processo=processo, parametros=parametros)
    logger.info("Execução %s iniciada (%s).", execucao.id_execucao, processo)

    erro: BaseException | None = None
    try:
        yield execucao
    except BaseException as excecao:
        erro = excecao
        raise  # Registro o erro, mas não o escondo: quem chamou precisa saber.
    finally:
        registro = montar_registro(execucao, erro, datetime.now(UTC))
        caminho = f"{CAMINHO_LOG}/{execucao.inicio:%Y%m%d_%H%M%S}_{execucao.id_execucao}.parquet"
        try:
            gravar_parquet(registro, caminho, config)
        except Exception:
            # Uma falha ao gravar o log nunca pode esconder o erro original da carga.
            logger.exception("Não consegui gravar o log da execução %s.", execucao.id_execucao)
        logger.info(
            "Execução %s terminou com status '%s': %d linha(s), %d chamada(s) à API.",
            execucao.id_execucao,
            registro["status"].iloc[0],
            execucao.linhas_novas,
            execucao.chamadas_api,
        )


def ler_log(config: ConfigLake, ultimas: int = 20) -> pd.DataFrame:
    """Devolve as últimas execuções registradas, da mais recente para a mais antiga."""
    if not existe(CAMINHO_LOG, config):
        return pd.DataFrame()
    log = ler_parquet(CAMINHO_LOG, config, schema=SCHEMA_LOG)
    return log.sort_values("inicio", ascending=False).head(ultimas).reset_index(drop=True)


if __name__ == "__main__":
    # Atalho para consultar o histórico: python -m ingestion.log_cargas
    logging.basicConfig(level=logging.WARNING)
    historico = ler_log(carregar_config())
    if historico.empty:
        print("Nenhuma execução registrada ainda.")
    else:
        colunas = [
            "inicio",
            "processo",
            "status",
            "linhas_novas",
            "chamadas_api",
            "duracao_segundos",
            "mensagem_erro",
        ]
        # Mostro o horário sem frações de segundo, para a tabela caber na tela.
        historico["inicio"] = historico["inicio"].dt.strftime("%Y-%m-%d %H:%M:%S")
        with pd.option_context("display.width", 200, "display.max_columns", None):
            print(historico[colunas].to_string(index=False))
