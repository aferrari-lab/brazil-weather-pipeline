{#-
    Por padrão, o dbt junta o schema do perfil com o da camada ("dbt_staging").
    Eu quero schemas com o nome exato da camada (staging, gold...), que é como o
    warehouse está organizado (o Spark já grava em "silver"). Esta macro troca o
    comportamento padrão: com +schema definido, usa ele puro; sem, usa o do perfil.
-#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
