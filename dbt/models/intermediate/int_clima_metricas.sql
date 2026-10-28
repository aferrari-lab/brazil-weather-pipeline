-- Métricas que dependem dos dias vizinhos: médias e acumulados móveis de 7 dias.
-- Ficam numa camada intermediária para não repetir a lógica em cada mart.
--
-- Uso RANGE (e não ROWS) na janela: ele considera os 7 dias de CALENDÁRIO até a
-- data, então um dia faltando não faz a janela "puxar" um dia mais antigo.

with clima as (

    select * from {{ ref('stg_clima_diario') }}

),

janelas as (

    select
        *,

        avg(temp_media_c) over semana as temp_media_movel_7d_c,
        sum(chuva_mm) over semana as chuva_acumulada_7d_mm,
        -- Quantos dias com dado existem na janela: abaixo de 7, a média móvel
        -- ainda está "aquecendo" (início do histórico) ou há dias faltando.
        count(temp_media_c) over semana as dias_na_janela_7d

    from clima
    window semana as (
        partition by codigo_ibge
        order by data
        range between interval '6 days' preceding and current row
    )

)

select
    *,
    {{ var('limite_dia_chuva_mm') }} <= coalesce(chuva_mm, 0) as dia_de_chuva
from janelas
