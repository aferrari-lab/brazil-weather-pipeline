{#-
    Teste genérico: os valores da coluna precisam estar entre um mínimo e um máximo.
    Nulos não são avaliados aqui (para isso existe o not_null).
    Uso no .yml:   tests: [entre: {minimo: 0, maximo: 100}]
    Qualquer um dos limites pode ser omitido (ex.: só minimo: 0).
-#}
{% test entre(model, column_name, minimo=none, maximo=none) %}

select {{ column_name }}
from {{ model }}
where {{ column_name }} is not null
  and (
      false
      {% if minimo is not none %} or {{ column_name }} < {{ minimo }} {% endif %}
      {% if maximo is not none %} or {{ column_name }} > {{ maximo }} {% endif %}
  )

{% endtest %}
