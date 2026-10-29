-- Staging do cadastro de municípios: uma linha por município.

with origem as (

    select * from {{ source('silver', 'municipios') }}

)

select
    codigo_ibge::bigint as codigo_ibge,
    municipio::varchar as municipio,
    uf_sigla::varchar(2) as uf_sigla,
    regiao::varchar as regiao,
    latitude::numeric(9, 6) as latitude,
    longitude::numeric(9, 6) as longitude

from origem
