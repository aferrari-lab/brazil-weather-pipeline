-- Features para prever a temperatura média de um dia a partir dos dias ANTERIORES.
--
-- Cada linha é um município num dia D: o alvo é a temperatura média de D, e todas
-- as features usam só informação até D-1 (o que já se sabia na véspera). Isso
-- evita o vazamento de dados mais comum em série temporal: usar o próprio dia
-- para prever o próprio dia.
--
-- Além dos dias que já têm dado, gero para cada município uma linha do dia
-- seguinte ao último disponível, com o alvo vazio: é a linha que o modelo
-- prevê de verdade (conjunto = 'futuro').

with base as (

    select
        f.codigo_ibge,
        f.data,
        f.temp_media_c::float8 as temp_media_c,
        f.temp_max_c::float8 as temp_max_c,
        f.temp_min_c::float8 as temp_min_c,
        f.chuva_mm::float8 as chuva_mm,
        f.umidade_media_pct::float8 as umidade_media_pct,
        f.vento_max_kmh::float8 as vento_max_kmh,
        f.temp_suspeita
    from {{ ref('fct_clima_diario') }} as f

),

proximo_dia as (

    -- O dia seguinte ao último dado de cada município, ainda sem medição.
    select
        codigo_ibge,
        max(data) + 1 as data,
        null::float8 as temp_media_c,
        null::float8 as temp_max_c,
        null::float8 as temp_min_c,
        null::float8 as chuva_mm,
        null::float8 as umidade_media_pct,
        null::float8 as vento_max_kmh,
        false as temp_suspeita
    from base
    group by codigo_ibge

),

serie as (

    select * from base
    union all
    select * from proximo_dia

),

com_historico as (

    select
        codigo_ibge,
        data,
        temp_media_c,
        temp_suspeita,
        lag(temp_media_c, 1) over w as temp_media_d1,
        lag(temp_media_c, 2) over w as temp_media_d2,
        lag(temp_media_c, 3) over w as temp_media_d3,
        lag(temp_media_c, 7) over w as temp_media_d7,
        lag(temp_max_c, 1) over w as temp_max_d1,
        lag(temp_min_c, 1) over w as temp_min_d1,
        lag(chuva_mm, 1) over w as chuva_d1,
        lag(umidade_media_pct, 1) over w as umidade_d1,
        lag(vento_max_kmh, 1) over w as vento_d1,
        -- Média dos 7 dias corridos ANTERIORES (não inclui o próprio dia).
        avg(temp_media_c) over (
            partition by codigo_ibge
            order by data
            range between interval '7 days' preceding and interval '1 day' preceding
        ) as temp_media_7d_anteriores,
        -- Para conferir que os "lags" são mesmo os dias anteriores, sem buraco.
        lag(data, 7) over w as data_d7
    from serie
    window w as (partition by codigo_ibge order by data)

)

select
    h.codigo_ibge,
    h.data,
    m.uf_sigla,
    m.regiao,
    m.latitude::float8 as latitude,
    m.longitude::float8 as longitude,
    extract(month from h.data)::int as mes,
    -- Sazonalidade como seno e cosseno do dia do ano: assim 31/12 e 1/1 ficam
    -- vizinhos, o que um número de 1 a 365 não mostra ao modelo.
    sin(2 * pi() * extract(doy from h.data) / 365.25) as sazonal_seno,
    cos(2 * pi() * extract(doy from h.data) / 365.25) as sazonal_cosseno,
    h.temp_media_d1,
    h.temp_media_d2,
    h.temp_media_d3,
    h.temp_media_d7,
    h.temp_media_7d_anteriores,
    h.temp_max_d1,
    h.temp_min_d1,
    h.chuva_d1,
    h.umidade_d1,
    h.vento_d1,
    h.temp_media_c as alvo_temp_media_c,
    case when h.temp_media_c is null then 'futuro' else 'historico' end
        as conjunto
from com_historico as h
inner join {{ ref('dim_municipio') }} as m on h.codigo_ibge = m.codigo_ibge
-- Só dias com os 7 anteriores completos (sem buraco na série)...
where h.data_d7 = h.data - 7
    -- ...e sem temperatura marcada como suspeita na silver, que ensinaria errado.
    and not coalesce(h.temp_suspeita, false)
