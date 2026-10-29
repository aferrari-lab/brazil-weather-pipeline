-- Cobertura por dia: em quantos municípios cada dia tem dado.
--
-- É o termômetro da carga: enquanto o Brasil está sendo carregado, a cobertura
-- sobe dia após dia; depois, qualquer queda num dia indica municípios que
-- deixaram de chegar. O denominador são os municípios com coordenada, porque
-- sem coordenada não há como buscar clima (ficam de fora, com aviso na ingestão).

with municipios as (

    select count(*) as municipios_buscaveis
    from {{ ref('dim_municipio') }}
    where latitude is not null and longitude is not null

),

por_dia as (

    select
        data,
        count(*)                               as municipios_com_dado,
        count(*) filter (where dado_completo)  as municipios_dado_completo,
        count(*) filter (where temp_suspeita)  as municipios_temp_suspeita
    from {{ ref('fct_clima_diario') }}
    group by data

),

ultimo as (

    select max(data) as ultimo_dia from {{ ref('fct_clima_diario') }}

)

select
    d.data,
    m.municipios_buscaveis,
    coalesce(p.municipios_com_dado, 0)          as municipios_com_dado,
    coalesce(p.municipios_dado_completo, 0)     as municipios_dado_completo,
    coalesce(p.municipios_temp_suspeita, 0)     as municipios_temp_suspeita,
    round(
        100.0 * coalesce(p.municipios_com_dado, 0) / nullif(m.municipios_buscaveis, 0), 1
    )                                           as pct_cobertura
from {{ ref('dim_data') }} as d
cross join municipios as m
cross join ultimo as u
left join por_dia as p on p.data = d.data
-- Até o último dia carregado: depois dele a falta é o atraso normal da API.
where d.data <= u.ultimo_dia
