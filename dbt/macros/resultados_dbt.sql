{#-
    Histórico das execuções do dbt: cada modelo e cada teste de cada "dbt build"
    vira uma linha em observabilidade.dbt_resultados (status, falhas, duração).

    O dbt só guarda o resultado da última execução (target/run_results.json); com
    este histórico dá para ver quando um teste começou a falhar e com que
    frequência. É o que pacotes como o elementary fazem, em versão enxuta.

    Ligadas no dbt_project.yml:
    - on-run-start → criar_tabela_resultados(): garante que a tabela existe;
    - on-run-end   → registrar_resultados(results): grava os resultados.
-#}

{% macro criar_tabela_resultados() %}
    create schema if not exists observabilidade;
    create table if not exists observabilidade.dbt_resultados (
        id_execucao        text,
        executado_em       timestamptz,
        comando            text,
        unique_id          text,
        tipo               text,
        nome               text,
        status             text,
        falhas             bigint,
        duracao_segundos   numeric(10, 2),
        mensagem           text
    );
{% endmacro %}


{% macro _texto_sql(valor, tamanho=1000) -%}
    {%- if valor is none -%}
        null
    {%- else -%}
        '{{ (valor | string)[:tamanho] | replace("'", "''") }}'
    {%- endif -%}
{%- endmacro %}


{% macro registrar_resultados(results) %}
    {#- Sem resultados (ex.: "dbt compile"), não há o que gravar. -#}
    {% if execute and results | length > 0 %}
        insert into observabilidade.dbt_resultados (
            id_execucao, executado_em, comando, unique_id, tipo, nome,
            status, falhas, duracao_segundos, mensagem
        )
        values
        {% for resultado in results -%}
            (
                '{{ invocation_id }}',
                '{{ run_started_at }}'::timestamptz,
                {{ _texto_sql(flags.WHICH) }},
                {{ _texto_sql(resultado.node.unique_id) }},
                {{ _texto_sql(resultado.node.resource_type) }},
                {{ _texto_sql(resultado.node.name) }},
                {{ _texto_sql(resultado.status) }},
                {{ resultado.failures if resultado.failures is not none else 'null' }},
                {{ resultado.execution_time | round(2) }},
                {{ _texto_sql(resultado.message) }}
            ){{ "," if not loop.last }}
        {% endfor %}
    {% endif %}
{% endmacro %}
