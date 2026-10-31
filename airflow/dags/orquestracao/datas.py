"""Data de referência de uma execução do Airflow."""

from datetime import UTC, date, datetime


def data_de_referencia(ds: str | None, disparada_em: datetime | None = None) -> date:
    """O dia que a execução representa.

    Nas execuções agendadas é a data lógica ({{ ds }}): refeita depois, a mesma
    execução pede o mesmo período. No Airflow 3, uma execução disparada à mão
    (botão Trigger ou "airflow dags trigger") não tem data lógica, e o ds chega
    vazio: aí uso o momento em que ela foi disparada.
    """
    if ds:
        return date.fromisoformat(ds)
    if disparada_em is not None:
        return disparada_em.date()
    return datetime.now(UTC).date()
