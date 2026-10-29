-- Teste singular: o fato precisa ter exatamente as mesmas chaves da staging.
-- Pega dois problemas que os testes genéricos não pegam: o incremental "pulando"
-- linhas (chave na staging que não chegou ao fato) e linhas que sobraram no fato
-- depois de sumirem da origem. Qualquer linha devolvida aqui é uma falha.

with staging as (
    select
        codigo_ibge,
        data
    from {{ ref('stg_clima_diario') }}
),

fato as (
    select
        codigo_ibge,
        data
    from {{ ref('fct_clima_diario') }}
)

select
    'falta no fato' as problema,
    s.*
from staging as s
left join fato as f on s.codigo_ibge = f.codigo_ibge and s.data = f.data
where f.codigo_ibge is null

union all

select
    'sobra no fato' as problema,
    f.*
from fato as f
left join staging as s on f.codigo_ibge = s.codigo_ibge and f.data = s.data
where s.codigo_ibge is null
