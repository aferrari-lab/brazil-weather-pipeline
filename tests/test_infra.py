"""Checagens rápidas da infraestrutura (não precisam do Docker rodando)."""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_compose_tem_servicos_essenciais():
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    servicos = set(compose["services"])
    esperados = {"postgres", "lake", "airflow-apiserver", "airflow-scheduler"}
    assert esperados <= servicos


def test_env_example_cobre_variaveis_do_compose():
    texto = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    env = (ROOT / ".env.example").read_text(encoding="utf-8")
    for var in ["WAREHOUSE_USER", "AIRFLOW_DB_USER", "LAKE_ACCESS_KEY", "AIRFLOW_JWT_SECRET"]:
        assert var in texto and f"{var}=" in env


def test_env_real_nao_versionado():
    assert ".env" in (ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()


def test_docker_proxy_nao_fica_exposto_fora_do_compose():
    # O proxy dá acesso ao Docker da máquina: não pode publicar porta no PC.
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    proxy = compose["services"]["docker-proxy"]
    assert "ports" not in proxy
    assert proxy["volumes"] == ["/var/run/docker.sock:/var/run/docker.sock:ro"]


def test_rede_tem_o_nome_que_as_dags_usam():
    from orquestracao.containers import REDE_DOCKER

    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    assert compose["networks"]["default"]["name"] == REDE_DOCKER
