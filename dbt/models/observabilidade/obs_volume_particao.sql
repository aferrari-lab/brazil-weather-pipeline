-- Volume por partição (UF × mês): linhas que chegaram contra as esperadas.
--
-- Aqui dá para saber exatamente quantas linhas deveriam existir: cada município
-- iniciado tem um dia por linha desde o início do histórico. Então, num mês já
-- fechado, uma UF deve ter (municípios iniciados × dias do mês) linhas. Menos que
-- isso é dado que se perdeu no caminho, e o teste
-- assert_particoes_com_volume_completo falha (o que vira alerta no Airflow).
--
-- "Fechado" é o mês que termina antes do último dia que TODOS os municípios
-- iniciados da UF já têm; o mês corrente ainda está chegando e não é cobrado.

with diario as (

    select
        f.codigo_ibge,
        f.data,
        m.uf_sigla
    from {{ ref('fct_clima_diario') }} as f
    inner join {{ ref('dim_municipio') }} as m on f.codigo_ibge = m.codigo_ibge

),

por_municipio as (

    select
        uf_sigla,
        codigo_ibge,
        max(data) as ultimo_dia
    from diario
    group by uf_sigla, codigo_ibge

),

por_uf as (

    select
        uf_sigla,
        count(*) as municipios_iniciados,
        min(ultimo_dia) as ultimo_dia_comum
    from por_municipio
    group by uf_sigla

),

meses as (

    select distinct
        ano,
        mes,
        date_trunc('month', data)::date as inicio_mes,
        (date_trunc('month', data) + interval '1 month - 1 day')::date as fim_mes
    from {{ ref('dim_data') }}

),

particoes as (

    select
        uf_sigla,
        extract(year from data)::int as ano,
        extract(month from data)::int as mes,
        count(*) as linhas
    from diario
    group by uf_sigla, ano, mes

),

calculado as (

    select
        u.uf_sigla,
        me.ano,
        me.mes,
        u.municipios_iniciados,
        me.fim_mes <= u.ultimo_dia_comum as mes_fechado,
        -- Dias do mês que todos os municípios iniciados já deveriam ter.
        (
            least(me.fim_mes, u.ultimo_dia_comum)
            - greatest(me.inicio_mes, '{{ var("data_inicio") }}'::date) + 1)
            as dias_considerados,
        coalesce(p.linhas, 0) as linhas
    from por_uf as u
    cross join meses as me
    left join particoes as p
        on u.uf_sigla = p.uf_sigla and me.ano = p.ano and me.mes = p.mes
    where me.inicio_mes <= u.ultimo_dia_comum

)

select
    uf_sigla,
    ano,
    mes,
    municipios_iniciados,
    mes_fechado,
    dias_considerados,
    linhas,
    municipios_iniciados * dias_considerados as linhas_esperadas,
    round(100.0 * linhas / nullif(municipios_iniciados * dias_considerados, 0), 1)
        as pct_do_esperado
from calculado
