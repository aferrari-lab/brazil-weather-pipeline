-- Resumo mensal por município, com a anomalia de temperatura do mês.
--
-- Anomalia = temperatura média do mês - média do mesmo mês do calendário no
-- histórico daquele município (ex.: março/2026 contra a média de todos os marços).
-- Ela mostra se o mês foi mais quente ou mais frio que o "normal" do lugar.
--
-- Esta tabela é recalculada inteira a cada execução (não é incremental): quando
-- chega um mês novo, a média histórica muda, e a anomalia dos meses antigos
-- precisa ser atualizada junto.
--
-- Limitação conhecida: com histórico desde 2025, a "média do mês" usa só 1 ou 2
-- anos. Na climatologia o padrão são 30 anos; com mais dados, o cálculo é o mesmo.

with diario as (

    select
        f.*,
        d.ano,
        d.mes
    from {{ ref('fct_clima_diario') }} as f
    inner join {{ ref('dim_data') }} as d on d.data = f.data

),

mensal as (

    select
        codigo_ibge,
        ano,
        mes,
        count(*)                                  as dias_com_dado,
        sum(case when dado_completo then 1 else 0 end) as dias_completos,
        round(avg(temp_media_c), 1)               as temp_media_c,
        max(temp_max_c)                           as temp_max_absoluta_c,
        min(temp_min_c)                           as temp_min_absoluta_c,
        round(sum(chuva_mm), 1)                   as chuva_total_mm,
        sum(case when dia_de_chuva then 1 else 0 end) as dias_de_chuva,
        round(avg(umidade_media_pct), 1)          as umidade_media_pct,
        max(vento_max_kmh)                        as vento_max_kmh
    from diario
    group by codigo_ibge, ano, mes

)

select
    *,
    -- Média do mesmo mês do calendário em todos os anos disponíveis do município.
    round(avg(temp_media_c) over (partition by codigo_ibge, mes), 1) as temp_media_historica_mes_c,
    round(temp_media_c - avg(temp_media_c) over (partition by codigo_ibge, mes), 1) as anomalia_temp_c,
    -- Quantos anos entraram na média histórica: deixa explícita a limitação acima.
    count(*) over (partition by codigo_ibge, mes)                    as anos_na_media_historica
from mensal
