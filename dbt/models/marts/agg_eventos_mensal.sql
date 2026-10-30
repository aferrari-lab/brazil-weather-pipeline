-- Resumo mensal dos eventos climáticos por município: quantos dias de cada evento
-- e quantos episódios distintos de onda de calor e de frio houve no mês.

with eventos as (

    select
        *,
        extract(year from data)::int as ano,
        extract(month from data)::int as mes,
        -- Um episódio começa no primeiro dia de onda depois de um dia sem onda.
        onda_de_calor
        and not coalesce(
            lag(onda_de_calor) over (partition by codigo_ibge order by data), false
        ) as inicio_onda_calor,
        onda_de_frio
        and not coalesce(
            lag(onda_de_frio) over (partition by codigo_ibge order by data), false
        ) as inicio_onda_frio
    from {{ ref('fct_eventos_climaticos') }}

)

select
    codigo_ibge,
    ano,
    mes,
    count(*) as dias_com_dado,
    count(*) filter (where onda_de_calor) as dias_onda_calor,
    count(*) filter (where inicio_onda_calor) as ondas_de_calor,
    count(*) filter (where onda_de_frio) as dias_onda_frio,
    count(*) filter (where inicio_onda_frio) as ondas_de_frio,
    count(*) filter (where chuva_extrema) as dias_chuva_extrema,
    count(*) filter (where estiagem) as dias_estiagem,
    max(dias_secos_seguidos) as maior_sequencia_seca,
    bool_and(limiar_confiavel) as limiares_confiaveis
from eventos
group by codigo_ibge, ano, mes
