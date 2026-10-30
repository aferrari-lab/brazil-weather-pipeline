-- Toda onda de calor ou de frio precisa ter pelo menos var('dias_minimos_onda')
-- dias seguidos, e todo dia de onda de calor precisa ser um dia quente (e o mesmo
-- para o frio). Qualquer linha devolvida aqui é uma falha na regra.

with ondas as (

    select
        codigo_ibge,
        data,
        onda_de_calor,
        dia_quente,
        onda_de_frio,
        dia_frio
    from {{ ref('fct_eventos_climaticos') }}

),

sequencias as (

    select
        codigo_ibge,
        data - (row_number() over (partition by codigo_ibge order by data))::int as grupo
    from ondas
    where onda_de_calor

)

select
    'onda de calor curta demais' as problema,
    codigo_ibge,
    min(grupo) as referencia
from sequencias
group by codigo_ibge, grupo
having count(*) < {{ var('dias_minimos_onda') }}

union all

select
    'dia de onda que não é extremo' as problema,
    codigo_ibge,
    min(data) as referencia
from ondas
where (onda_de_calor and not dia_quente) or (onda_de_frio and not dia_frio)
group by codigo_ibge
