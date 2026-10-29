-- Dimensão de datas: um dia por linha, do início do histórico até hoje.
-- Gero a sequência de datas no próprio banco (generate_series), em vez de
-- depender dos dias que existem no clima: assim um dia sem dado continua
-- aparecendo nos relatórios, e a falta fica visível em vez de sumir.

with dias as (

    select dia::date as data
    from generate_series(
        '{{ var("data_inicio") }}'::date,
        current_date,
        interval '1 day'
    ) as dia

)

select
    data,
    extract(year from data)::int as ano,
    extract(quarter from data)::int as trimestre,
    extract(month from data)::int as mes,
    (
        array[
            'Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho', 'Julho',
            'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro'
        ]
    )[extract(month from data)::int] as nome_mes,
    extract(day from data)::int as dia,
    -- isodow: 1 = segunda-feira ... 7 = domingo
    extract(isodow from data)::int as dia_semana,
    (array['Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado', 'Domingo']
    )[extract(isodow from data)::int] as nome_dia_semana,
    extract(isodow from data) in (6, 7) as fim_de_semana,
    -- Estações meteorológicas do hemisfério sul (por mês, como na climatologia).
    case
        when extract(month from data) in (12, 1, 2) then 'Verão'
        when extract(month from data) in (3, 4, 5) then 'Outono'
        when extract(month from data) in (6, 7, 8) then 'Inverno'
        else 'Primavera'
    end as estacao
from dias
