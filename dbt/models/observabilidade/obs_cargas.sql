-- Uma linha por execução da ingestão, a partir do log que o Airflow copia do lake
-- para ops.log_cargas (tarefa publicar_log_cargas).
--
-- Se a tabela ainda não existe (antes da primeira execução pelo Airflow), o
-- modelo sai vazio em vez de quebrar o "dbt build".
-- depends_on: {{ source('ops', 'log_cargas') }}

{% set tabela_log = adapter.get_relation(
    database=target.database, schema='ops', identifier='log_cargas'
) %}

{% if tabela_log is not none %}

select
    id_execucao,
    processo,
    status,
    inicio,
    fim,
    (inicio at time zone 'America/Sao_Paulo')::date as dia,
    round((duracao_segundos / 60.0)::numeric, 1) as duracao_minutos,
    linhas_novas,
    chamadas_api,
    parametros,
    mensagem_erro
from {{ source('ops', 'log_cargas') }}

{% else %}

select
    null::text         as id_execucao,
    null::text         as processo,
    null::text         as status,
    null::timestamptz  as inicio,
    null::timestamptz  as fim,
    null::date         as dia,
    null::numeric      as duracao_minutos,
    null::bigint       as linhas_novas,
    null::bigint       as chamadas_api,
    null::text         as parametros,
    null::text         as mensagem_erro
where false

{% endif %}
