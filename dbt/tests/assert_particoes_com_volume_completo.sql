-- Alerta de perda de dados: em mês fechado, cada UF precisa ter exatamente
-- (municípios iniciados × dias do mês) linhas. Se faltar alguma, o teste falha,
-- o "dbt build" falha e o Airflow manda o alerta de falha.
--
-- As linhas que faltaram ficam em testes_falhas, para investigar a partição.

select
    uf_sigla,
    ano,
    mes,
    linhas,
    linhas_esperadas,
    linhas_esperadas - linhas as linhas_faltando
from {{ ref('obs_volume_particao') }}
where mes_fechado
    and linhas < linhas_esperadas
