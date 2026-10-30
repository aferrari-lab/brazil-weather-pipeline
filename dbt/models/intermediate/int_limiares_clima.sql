-- Limiares de extremo de cada município em cada mês do calendário.
--
-- Um dia é "quente" comparado com o normal DAQUELE lugar NAQUELA época do ano:
-- 32 °C é um dia comum em Cuiabá em outubro e um dia extremo em Curitiba em julho.
-- Por isso os percentis são calculados por município e por mês, sobre todo o
-- histórico disponível (como nos índices de extremos do ETCCDI, que usam um
-- período de referência).
--
-- Limitação conhecida: com histórico desde 2025, cada mês tem só 1 ou 2 anos de
-- base. A coluna limiar_confiavel marca os meses com pelo menos
-- var('dias_minimos_base_limiar') dias de base.

with diario as (

    select
        codigo_ibge,
        extract(month from data)::int as mes,
        extract(year from data)::int as ano,
        temp_max_c,
        temp_min_c,
        chuva_mm
    from {{ ref('fct_clima_diario') }}

)

select
    codigo_ibge,
    mes,
    count(*) as dias_na_base,
    count(distinct ano) as anos_na_base,
    percentile_cont({{ var('percentil_dia_quente') }}) within group (order by temp_max_c)
        as limiar_temp_max_c,
    percentile_cont({{ var('percentil_dia_frio') }}) within group (order by temp_min_c)
        as limiar_temp_min_c,
    -- Chuva extrema é medida só entre os dias que choveram: num mês seco, quase
    -- todo dia tem 0 mm, e o percentil de todos os dias seria sempre zero.
    percentile_cont({{ var('percentil_chuva_extrema') }}) within group (
        order by chuva_mm
    ) filter (where chuva_mm >= {{ var('limite_dia_chuva_mm') }}) as limiar_chuva_mm,
    count(*) >= {{ var('dias_minimos_base_limiar') }} as limiar_confiavel
from diario
group by codigo_ibge, mes
