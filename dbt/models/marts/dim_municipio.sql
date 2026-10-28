-- Dimensão de municípios: uma linha por município, com UF, região e coordenadas.

select
    codigo_ibge,
    municipio,
    uf_sigla,
    regiao,
    latitude,
    longitude
from {{ ref('stg_municipios') }}
