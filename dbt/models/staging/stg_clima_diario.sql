-- Staging do clima: uma linha por município e dia, como na silver.
-- Aqui eu só seleciono, tipo e padronizo nomes. Nenhuma regra de negócio entra
-- na staging; elas ficam nas camadas seguintes (intermediate e marts).

with origem as (

    select * from {{ source('silver', 'clima_diario') }}

)

select
    -- Chaves
    codigo_ibge::bigint                    as codigo_ibge,
    data::date                             as data,
    uf_sigla::varchar(2)                   as uf_sigla,

    -- Medidas (a unidade está no nome da coluna)
    temp_max_c::numeric(5, 1)              as temp_max_c,
    temp_min_c::numeric(5, 1)              as temp_min_c,
    temp_media_c::numeric(5, 1)            as temp_media_c,
    amplitude_termica_c::numeric(5, 1)     as amplitude_termica_c,
    chuva_mm::numeric(6, 1)                as chuva_mm,
    umidade_media_pct::numeric(5, 1)       as umidade_media_pct,
    vento_max_kmh::numeric(5, 1)           as vento_max_kmh,

    -- Qualidade (calculada na silver)
    dado_completo::boolean                 as dado_completo,
    temp_suspeita::boolean                 as temp_suspeita,

    -- Controle: quando o dado foi gravado no lake e processado na silver
    _gravado_em                            as gravado_em,
    _processado_em                         as processado_em

from origem
