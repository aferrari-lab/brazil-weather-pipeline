"""Aviso de falha das tarefas: sempre no log e, se configurado, no Telegram.

Para receber no Telegram, crie um bot com o @BotFather e coloque no .env:
    TELEGRAM_BOT_TOKEN=...
    TELEGRAM_CHAT_ID=...
Sem essas variáveis, o aviso fica só no log (e na tela do Airflow, em vermelho).
"""

import logging
import os
from collections.abc import Mapping
from typing import Any

import requests

logger = logging.getLogger(__name__)

URL_TELEGRAM = "https://api.telegram.org/bot{token}/sendMessage"
TAMANHO_MAXIMO_ERRO = 300


def montar_mensagem(contexto: Mapping[str, Any]) -> str:
    """Resume a falha numa mensagem curta, legível no celular."""
    ti = contexto.get("ti") or contexto.get("task_instance")
    erro = str(contexto.get("exception") or "sem detalhe")[:TAMANHO_MAXIMO_ERRO]
    return (
        f"Falha no pipeline de clima\n"
        f"DAG: {ti.dag_id}\n"
        f"Tarefa: {ti.task_id} (tentativa {ti.try_number})\n"
        f"Execução: {ti.run_id}\n"
        f"Erro: {erro}"
    )


def enviar_telegram(mensagem: str, ambiente: Mapping[str, str] | None = None) -> bool:
    """Manda a mensagem pelo Telegram, se o bot estiver configurado."""
    ambiente = os.environ if ambiente is None else ambiente
    token, chat_id = ambiente.get("TELEGRAM_BOT_TOKEN"), ambiente.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        return False
    resposta = requests.post(
        URL_TELEGRAM.format(token=token),
        json={"chat_id": chat_id, "text": mensagem},
        timeout=10,
    )
    resposta.raise_for_status()
    return True


def avisar_falha(contexto: Mapping[str, Any]) -> None:
    """Callback de falha das DAGs (on_failure_callback)."""
    mensagem = montar_mensagem(contexto)
    logger.error(mensagem)
    try:
        enviar_telegram(mensagem)
    except requests.RequestException:
        # O alerta nunca pode derrubar nada: se o Telegram falhar, fica só o log.
        logger.exception("Não consegui enviar o alerta pelo Telegram.")
