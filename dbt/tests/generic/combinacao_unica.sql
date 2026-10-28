{#-
    Teste genérico: a combinação das colunas não pode se repetir.
    Uso para a chave composta do clima (município + dia). Escrevi o meu em vez
    de instalar um pacote só para isso: são poucas linhas e não dependem de nada.
    Uso no .yml:   tests: [combinacao_unica: {colunas: [codigo_ibge, data]}]
-#}
{% test combinacao_unica(model, colunas) %}

select
    {{ colunas | join(', ') }},
    count(*) as ocorrencias
from {{ model }}
group by {{ colunas | join(', ') }}
having count(*) > 1

{% endtest %}
