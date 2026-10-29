-- Situação da carga por UF: quantos municípios já têm clima e até que dia.
--
-- Um município "iniciado" já recebeu o histórico inteiro (cada chamada à API
-- traz o período todo de um lote de municípios). Por isso a UF fica "completa"
-- quando todos os seus municípios buscáveis foram iniciados.

with municipios as (

    select
        uf_sigla,
        regiao,
        count(*)                                                        as municipios_total,
        count(*) filter (where latitude is not null and longitude is not null)
                                                                        as municipios_buscaveis
    from {{ ref('dim_municipio') }}
    group by uf_sigla, regiao

),

por_municipio as (

    select
        m.uf_sigla,
        f.codigo_ibge,
        min(f.data)  as primeiro_dia,
        max(f.data)  as ultimo_dia,
        count(*)     as dias
    from {{ ref('fct_clima_diario') }} as f
    inner join {{ ref('dim_municipio') }} as m on m.codigo_ibge = f.codigo_ibge
    group by m.uf_sigla, f.codigo_ibge

),

por_uf as (

    select
        uf_sigla,
        count(*)           as municipios_iniciados,
        min(primeiro_dia)  as primeiro_dia,
        max(ultimo_dia)    as ultimo_dia,
        sum(dias)          as linhas
    from por_municipio
    group by uf_sigla

)

select
    m.uf_sigla,
    m.regiao,
    m.municipios_total,
    m.municipios_buscaveis,
    m.municipios_total - m.municipios_buscaveis           as municipios_sem_coordenada,
    coalesce(u.municipios_iniciados, 0)                   as municipios_iniciados,
    round(
        100.0 * coalesce(u.municipios_iniciados, 0) / nullif(m.municipios_buscaveis, 0), 1
    )                                                     as pct_municipios_iniciados,
    u.primeiro_dia,
    u.ultimo_dia,
    coalesce(u.linhas, 0)                                 as linhas,
    case
        when u.municipios_iniciados is null then 'não iniciada'
        when u.municipios_iniciados < m.municipios_buscaveis then 'em carga'
        else 'completa'
    end                                                   as situacao
from municipios as m
left join por_uf as u on u.uf_sigla = m.uf_sigla
