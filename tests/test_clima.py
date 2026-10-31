"""Testes do extrator de clima, sem internet e sem o lake real.

Eu simulo a API do Open-Meteo com uma sessão falsa, que devolve respostas no
mesmo formato da real, e gravo num lake local numa pasta temporária.
"""

from datetime import UTC, date, datetime

import pandas as pd
import pytest

from ingestion import clima
from ingestion.lake import ConfigLake, ler_parquet

INICIO, FIM = date(2025, 1, 30), date(2025, 2, 2)  # 4 dias, cruzando a virada do mês
EXTRAIDO_EM = datetime(2026, 10, 28, 12, 0, tzinfo=UTC)

MUNICIPIOS = pd.DataFrame(
    {
        "codigo_ibge": [3509502, 3550308, 3304557],
        "nome": ["Campinas", "São Paulo", "Rio de Janeiro"],
        "uf_sigla": ["SP", "SP", "RJ"],
        "latitude": [-22.9053, -23.5329, -22.9129],
        "longitude": [-47.0659, -46.6395, -43.2003],
    }
)


def _resposta(
    inicio: date = INICIO, fim: date = FIM, base: float = 25.0, dias_nulos: int = 0
) -> dict:
    """Uma resposta da API para um ponto, no formato real do Open-Meteo.

    dias_nulos: os últimos N dias vêm sem valor, como acontece quando a
    reanálise daquele período ainda não ficou pronta.
    """
    dias = pd.date_range(inicio, fim).strftime("%Y-%m-%d").tolist()
    n = len(dias)

    def serie(valor):
        return [valor] * (n - dias_nulos) + [None] * dias_nulos

    return {
        "latitude": -22.9,
        "longitude": -47.0,
        "daily": {
            "time": dias,
            "temperature_2m_max": serie(base + 5),
            "temperature_2m_min": serie(base - 5),
            "temperature_2m_mean": serie(base),
            "precipitation_sum": serie(0.0),
            "relative_humidity_2m_mean": serie(70.0),
            "wind_speed_10m_max": serie(12.0),
        },
    }


class _RespostaFalsa:
    def __init__(self, dados):
        self._dados = dados

    def raise_for_status(self):
        pass

    def json(self):
        return self._dados


class _SessaoFalsa:
    """Imita requests.Session: responde às datas pedidas e guarda as chamadas feitas."""

    def __init__(self, dias_nulos: int = 0):
        self.chamadas = []
        self.dias_nulos = dias_nulos

    def get(self, url, params, timeout):
        self.chamadas.append(params)
        n_pontos = len(params["latitude"].split(","))
        inicio = date.fromisoformat(params["start_date"])
        fim = date.fromisoformat(params["end_date"])
        dados = [_resposta(inicio, fim, dias_nulos=self.dias_nulos) for _ in range(n_pontos)]
        # Igual à API real: um ponto devolve um objeto, vários devolvem lista.
        return _RespostaFalsa(dados[0] if n_pontos == 1 else dados)


# --- Período e tamanho dos lotes ---
def test_periodo_padrao_respeita_o_atraso_dos_dados():
    assert clima.periodo_padrao(date(2026, 10, 28)) == (date(2025, 1, 1), date(2026, 10, 23))


def test_lote_fica_dentro_do_peso_por_minuto():
    inicio, fim = date(2025, 1, 1), date(2026, 10, 23)
    tamanho = clima.tamanho_do_lote(inicio, fim)

    assert tamanho * clima.peso_por_local(inicio, fim) <= clima.PESO_MAXIMO_POR_MINUTO
    assert tamanho >= 1


def test_periodo_curto_pesa_o_minimo():
    # Até 14 dias e 10 variáveis, cada ponto conta como 1 chamada.
    assert clima.peso_por_local(date(2025, 1, 1), date(2025, 1, 1)) == 1.0


# --- Extração ---
def test_extrair_monta_os_parametros_e_padroniza_um_ponto_como_lista():
    sessao = _SessaoFalsa()
    respostas = clima.extrair_clima(MUNICIPIOS.head(1), INICIO, FIM, sessao)

    assert isinstance(respostas, list) and len(respostas) == 1
    params = sessao.chamadas[0]
    assert params["latitude"] == "-22.9053"
    assert params["timezone"] == "auto"
    assert params["daily"].split(",") == clima.VARIAVEIS_DIARIAS


def test_extrair_falha_se_a_api_devolver_quantidade_diferente():
    class SessaoIncompleta(_SessaoFalsa):
        def get(self, url, params, timeout):
            return _RespostaFalsa([_resposta()])  # sempre 1, mesmo pedindo 3

    with pytest.raises(ValueError, match="devolveu 1"):
        clima.extrair_clima(MUNICIPIOS, INICIO, FIM, SessaoIncompleta())


# --- Transformação ---
def test_transformar_gera_uma_linha_por_municipio_e_dia():
    lote = MUNICIPIOS.head(2)
    df = clima.transformar_clima([_resposta(), _resposta(base=20)], lote, EXTRAIDO_EM)

    assert len(df) == 2 * 4
    assert df["data"].iloc[0] == INICIO
    assert set(df["mes"]) == {1, 2}  # cruzou a virada de janeiro para fevereiro
    assert df.loc[df["codigo_ibge"] == 3550308, "temperature_2m_mean"].iloc[0] == 20.0


def test_transformar_falha_se_faltar_variavel():
    resposta = _resposta()
    del resposta["daily"]["precipitation_sum"]

    with pytest.raises(ValueError, match="precipitation_sum"):
        clima.transformar_clima([resposta], MUNICIPIOS.head(1), EXTRAIDO_EM)


# --- Validação ---
def test_validar_barra_linhas_duplicadas():
    df = clima.transformar_clima([_resposta()], MUNICIPIOS.head(1), EXTRAIDO_EM)
    duplicado = pd.concat([df, df.head(1)], ignore_index=True)

    with pytest.raises(ValueError, match="duplicada"):
        clima.validar_clima(duplicado, 1, INICIO, FIM)


def test_validar_barra_datas_fora_do_periodo():
    df = clima.transformar_clima([_resposta()], MUNICIPIOS.head(1), EXTRAIDO_EM)

    with pytest.raises(ValueError, match="fora do período"):
        clima.validar_clima(df, 1, INICIO, date(2025, 1, 31))


# --- Pendências (o que falta buscar) ---
def test_pendencias_sem_nada_carregado_comeca_no_inicio():
    pendentes = clima.calcular_pendencias(MUNICIPIOS, clima._ultimas_datas_vazio(), INICIO, FIM)

    assert len(pendentes) == 3
    assert set(pendentes["inicio_pendente"]) == {INICIO}


def test_pendencias_continua_do_dia_seguinte_e_pula_quem_esta_em_dia():
    checkpoint = pd.DataFrame(
        {"codigo_ibge": [3509502, 3550308], "ultima_data": [date(2025, 1, 31), FIM]}
    )
    pendentes = clima.calcular_pendencias(MUNICIPIOS, checkpoint, INICIO, FIM)
    inicio_por_cidade = dict(
        zip(pendentes["codigo_ibge"], pendentes["inicio_pendente"], strict=True)
    )

    # Campinas parou em 31/01: continua em 01/02. São Paulo está em dia: some da
    # lista. Rio nunca foi carregado: começa no início do período.
    assert inicio_por_cidade == {3509502: date(2025, 2, 1), 3304557: INICIO}


# --- Carga completa de uma UF ---
def _lake(tmp_path) -> ConfigLake:
    return ConfigLake(raiz=str(tmp_path / "lake"))


SP = MUNICIPIOS[MUNICIPIOS["uf_sigla"] == "SP"]


def test_carga_grava_particoes_e_segunda_execucao_nao_busca_nada(tmp_path, monkeypatch):
    lake = _lake(tmp_path)
    # Forço lotes de 1 município para testar vários lotes e o checkpoint por lote.
    monkeypatch.setattr(clima, "PESO_MAXIMO_POR_MINUTO", 1)
    sessao = _SessaoFalsa()

    primeira = clima.carregar_clima_uf(SP, INICIO, FIM, sessao, lake, pausa_segundos=0)
    segunda = clima.carregar_clima_uf(SP, INICIO, FIM, sessao, lake, pausa_segundos=0)

    assert primeira == 2 * 4
    assert segunda == 0  # tudo em dia: nenhuma chamada à API
    assert len(sessao.chamadas) == 2  # só os 2 lotes da primeira execução
    assert len(ler_parquet(clima.CAMINHO_CLIMA, lake)) == 2 * 4
    assert (tmp_path / "lake/bronze/clima_diario/ano=2025/mes=2/uf_sigla=SP").is_dir()


def test_carga_incremental_busca_so_os_dias_novos_e_preserva_os_antigos(tmp_path):
    lake = _lake(tmp_path)
    sessao = _SessaoFalsa()

    # Mês 1: carrego até 02/02. Mês seguinte: o período vai até 05/02.
    clima.carregar_clima_uf(SP, INICIO, FIM, sessao, lake, pausa_segundos=0)
    novas = clima.carregar_clima_uf(SP, INICIO, date(2025, 2, 5), sessao, lake, pausa_segundos=0)

    assert sessao.chamadas[-1]["start_date"] == "2025-02-03"  # só o que faltava
    assert novas == 2 * 3

    lido = ler_parquet(clima.CAMINHO_CLIMA, lake)
    assert len(lido) == 2 * 7  # 30/01 a 05/02, sem duplicar
    # A partição de fevereiro tem que manter 01 e 02/02, gravados na primeira carga.
    fevereiro = lido[lido["mes"].astype(int) == 2]
    assert sorted(set(pd.to_datetime(fevereiro["data"]).dt.day)) == [1, 2, 3, 4, 5]


def test_carga_retoma_de_onde_parou_apos_interrupcao(tmp_path, monkeypatch):
    lake = _lake(tmp_path)
    monkeypatch.setattr(clima, "PESO_MAXIMO_POR_MINUTO", 1)  # 1 município por lote

    class SessaoQueCai(_SessaoFalsa):
        """Funciona na primeira chamada e falha na segunda, como no limite diário."""

        def get(self, url, params, timeout):
            if len(self.chamadas) == 1:
                raise RuntimeError("limite da API")
            return super().get(url, params, timeout)

    with pytest.raises(RuntimeError):
        clima.carregar_clima_uf(SP, INICIO, FIM, SessaoQueCai(), lake, pausa_segundos=0)

    # O primeiro lote ficou salvo. Na nova execução, só o segundo município é buscado.
    sessao = _SessaoFalsa()
    clima.carregar_clima_uf(SP, INICIO, FIM, sessao, lake, pausa_segundos=0)

    assert len(sessao.chamadas) == 1
    assert len(ler_parquet(clima.CAMINHO_CLIMA, lake)) == 2 * 4


def _versao_mais_recente(bronze: pd.DataFrame) -> pd.DataFrame:
    """O que a silver enxerga: uma linha por município e dia, a gravada por último."""
    return bronze.sort_values("_gravado_em").drop_duplicates(["codigo_ibge", "data"], keep="last")


def test_dias_ainda_nulos_sao_buscados_de_novo(tmp_path):
    lake = _lake(tmp_path)

    # Os 2 últimos dias vêm nulos (dado ainda não disponível na API).
    clima.carregar_clima_uf(SP, INICIO, FIM, _SessaoFalsa(dias_nulos=2), lake, pausa_segundos=0)
    sessao = _SessaoFalsa()
    clima.carregar_clima_uf(SP, INICIO, FIM, sessao, lake, pausa_segundos=0)

    # A segunda execução pede de novo só os 2 dias que vieram nulos e os completa.
    assert sessao.chamadas[0]["start_date"] == "2025-02-01"
    atual = _versao_mais_recente(ler_parquet(clima.CAMINHO_CLIMA, lake))
    assert len(atual) == 2 * 4
    assert atual["temperature_2m_mean"].notna().all()


def test_reprocessar_busca_de_novo_e_a_versao_nova_prevalece(tmp_path):
    lake = _lake(tmp_path)
    clima.carregar_clima_uf(SP, INICIO, FIM, _SessaoFalsa(), lake, pausa_segundos=0)

    sessao = _SessaoFalsa()
    clima.carregar_clima_uf(SP, INICIO, FIM, sessao, lake, pausa_segundos=0, reprocessar=True)

    bronze = ler_parquet(clima.CAMINHO_CLIMA, lake)
    assert len(sessao.chamadas) == 1  # buscou de novo
    assert len(bronze) == 2 * 2 * 4  # o bronze guarda as duas versões (append-only)
    assert len(_versao_mais_recente(bronze)) == 2 * 4  # a silver vê uma linha por dia


# --- Garantias do bronze append-only ---
def test_cada_lote_grava_arquivos_novos_sem_tocar_nos_antigos(tmp_path):
    lake = _lake(tmp_path)
    pasta = tmp_path / "lake" / clima.CAMINHO_CLIMA

    clima.carregar_clima_uf(SP, INICIO, FIM, _SessaoFalsa(), lake, pausa_segundos=0)
    antes = {arquivo: arquivo.read_bytes() for arquivo in pasta.rglob("*.parquet")}
    clima.carregar_clima_uf(
        SP, INICIO, FIM, _SessaoFalsa(), lake, pausa_segundos=0, reprocessar=True
    )
    depois = list(pasta.rglob("*.parquet"))

    assert len(depois) == 2 * len(antes)  # arquivos novos para o novo lote
    assert all(arquivo.read_bytes() == conteudo for arquivo, conteudo in antes.items())


def test_ufs_carregadas_em_paralelo_nao_se_atrapalham(tmp_path):
    # Simulo duas cargas simultâneas: as duas calculam o que falta ANTES de qualquer
    # uma gravar (o pior caso de concorrência) e depois gravam.
    lake = _lake(tmp_path)
    rj = MUNICIPIOS[MUNICIPIOS["uf_sigla"] == "RJ"]
    pendentes_sp = clima.ultimas_datas_carregadas(lake, "SP")
    pendentes_rj = clima.ultimas_datas_carregadas(lake, "RJ")
    assert pendentes_sp.empty and pendentes_rj.empty

    clima.carregar_clima_uf(SP, INICIO, FIM, _SessaoFalsa(), lake, pausa_segundos=0)
    clima.carregar_clima_uf(rj, INICIO, FIM, _SessaoFalsa(), lake, pausa_segundos=0)

    # Nenhuma carga apagou a outra, e o "checkpoint" de cada UF reflete só a dela.
    assert len(ler_parquet(clima.CAMINHO_CLIMA, lake)) == 3 * 4
    assert set(clima.ultimas_datas_carregadas(lake, "SP")["codigo_ibge"]) == {3509502, 3550308}
    assert set(clima.ultimas_datas_carregadas(lake, "RJ")["codigo_ibge"]) == {3304557}


def test_lote_registra_a_hora_da_gravacao(tmp_path):
    lake = _lake(tmp_path)
    clima.carregar_clima_uf(SP, INICIO, FIM, _SessaoFalsa(), lake, pausa_segundos=0)

    bronze = ler_parquet(clima.CAMINHO_CLIMA, lake)

    assert bronze["_gravado_em"].notna().all()
    assert (bronze["_gravado_em"] >= bronze["_extraido_em"]).all()


def test_carga_da_uf_ignora_municipio_sem_coordenada(tmp_path):
    sp = SP.copy()
    sp.loc[sp.index[0], "latitude"] = None

    linhas = clima.carregar_clima_uf(sp, INICIO, FIM, _SessaoFalsa(), _lake(tmp_path), 0)

    assert linhas == 1 * 4


# --- Respostas inválidas da API ---
class _RespostaTexto:
    """Uma resposta "200 OK" cujo conteúdo não é JSON (ex.: página de aviso)."""

    status_code = 200
    headers = {"Content-Type": "text/html"}
    text = "<html>Servico temporariamente indisponivel</html>"

    def raise_for_status(self):
        pass

    def json(self):
        raise ValueError("Expecting value: line 1 column 1 (char 0)")


def test_resposta_que_nao_e_json_explica_o_que_chegou():
    class SessaoComAviso(_SessaoFalsa):
        def get(self, url, params, timeout):
            return _RespostaTexto()

    with pytest.raises(clima.RespostaInvalidaError, match="text/html.*indisponivel"):
        clima.extrair_clima(MUNICIPIOS.head(1), INICIO, FIM, SessaoComAviso())


def test_erro_informado_pela_api_vira_excecao_clara():
    class SessaoComErro(_SessaoFalsa):
        def get(self, url, params, timeout):
            return _RespostaFalsa({"error": True, "reason": "Daily API request limit exceeded"})

    with pytest.raises(clima.RespostaInvalidaError, match="Daily API request limit"):
        clima.extrair_clima(MUNICIPIOS.head(1), INICIO, FIM, SessaoComErro())


# --- Lotes da carga diária (poucos dias, muitos municípios) ---
def test_carga_de_um_dia_respeita_o_maximo_de_municipios_por_chamada():
    # Com 1 dia, o peso por município é o mínimo e caberiam 500 numa chamada, mas
    # a URL com 500 coordenadas passa do limite da API (erro 414).
    dia = date(2026, 10, 26)

    assert clima.tamanho_do_lote(dia, dia) == clima.MAX_MUNICIPIOS_POR_CHAMADA


def test_url_do_lote_maximo_cabe_no_limite():
    lote = pd.DataFrame(
        {
            "latitude": [-23.5503] * clima.MAX_MUNICIPIOS_POR_CHAMADA,
            "longitude": [-46.6333] * clima.MAX_MUNICIPIOS_POR_CHAMADA,
        }
    )
    coordenadas = ",".join(f"{v:.4f}" for v in lote["latitude"]) + ",".join(
        f"{v:.4f}" for v in lote["longitude"]
    )
    # Vírgulas viram %2C na URL (3 caracteres): conto o pior caso.
    assert len(coordenadas.replace(",", "%2C")) + 400 < 4000


def test_pausa_proporcional_ao_peso_do_lote():
    dia = date(2026, 10, 26)
    historico = (date(2025, 1, 1), date(2026, 10, 23))

    assert clima.pausa_do_lote(100, dia, dia, 60) == pytest.approx(12)  # peso 100 de 500
    lote_cheio = clima.tamanho_do_lote(*historico)
    assert clima.pausa_do_lote(lote_cheio, *historico, 60) <= 60
    assert clima.pausa_do_lote(5000, dia, dia, 60) == 60  # nunca passa da pausa cheia
