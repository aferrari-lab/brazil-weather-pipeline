"""Checagens rápidas da infraestrutura (não precisam do Docker rodando)."""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_compose_tem_servicos_essenciais():
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    servicos = set(compose["services"])
    esperados = {"postgres", "minio", "airflow-apiserver", "airflow-scheduler"}
    assert esperados <= servicos


def test_env_example_cobre_variaveis_do_compose():
    texto = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    env = (ROOT / ".env.example").read_text(encoding="utf-8")
    for var in ["WAREHOUSE_USER", "AIRFLOW_DB_USER", "MINIO_ROOT_USER", "AIRFLOW_JWT_SECRET"]:
        assert var in texto and f"{var}=" in env


def test_env_real_nao_versionado():
    assert ".env" in (ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
