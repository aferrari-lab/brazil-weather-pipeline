-- Histórico dos testes do dbt: uma linha por teste em cada execução.
--
-- Vem de observabilidade.dbt_resultados, gravada pelo próprio dbt ao fim de cada
-- execução (macros/resultados_dbt.sql). Como este modelo roda antes desse
-- registro, ele mostra as execuções ANTERIORES à atual.

select
    id_execucao,
    executado_em,
    (executado_em at time zone 'America/Sao_Paulo')::date  as dia,
    comando,
    nome                                                    as teste,
    status,
    coalesce(falhas, 0)                                     as falhas,
    duracao_segundos,
    mensagem
from {{ source('observabilidade', 'dbt_resultados') }}
where tipo = 'test'
