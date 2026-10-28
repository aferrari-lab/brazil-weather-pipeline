-- Fato do clima diário: uma linha por município e dia, ligada às dimensões.
--
-- Incremental: depois da primeira carga, cada execução processa só as linhas que
-- a silver (re)processou desde a última vez, e não a tabela inteira. A estratégia
-- delete+insert apaga as chaves que chegaram de novo e insere a versão atual,
-- então um dia reprocessado na silver é atualizado aqui, sem duplicar.
--
-- As médias móveis vêm calculadas da camada intermediária sobre o histórico
-- inteiro, e só depois eu filtro o que precisa ser reprocessado. Isso inclui um
-- cuidado importante: quando um dia muda, a média móvel dos 6 dias SEGUINTES
-- também muda (a janela deles inclui o dia alterado). Então reprocesso o dia
-- alterado e os 6 seguintes daquele município, e não só o dia em si. Um teste
-- comparando o incremental com uma reconstrução completa pegou esse caso.

{{
    config(
        materialized='incremental',
        unique_key=['codigo_ibge', 'data'],
        incremental_strategy='delete+insert',
        on_schema_change='append_new_columns'
    )
}}

with metricas as (

    select * from {{ ref('int_clima_metricas') }}

),

{% if is_incremental() %}

alterados as (

    -- Dias que a silver (re)processou depois da última carga desta tabela.
    select codigo_ibge, data
    from metricas
    where processado_em > (select coalesce(max(processado_em), '1900-01-01') from {{ this }})

),

a_reprocessar as (

    -- Cada dia alterado e os 6 dias seguintes do mesmo município: são as linhas
    -- cuja janela de 7 dias inclui algum dia alterado.
    select distinct m.*
    from metricas as m
    inner join alterados as a
        on m.codigo_ibge = a.codigo_ibge
        and m.data between a.data and a.data + 6

)

{% else %}

a_reprocessar as (

    select * from metricas

)

{% endif %}

select
    -- Chaves (ligam o fato às dimensões)
    codigo_ibge,
    data,

    -- Medidas do dia
    temp_max_c,
    temp_min_c,
    temp_media_c,
    amplitude_termica_c,
    chuva_mm,
    umidade_media_pct,
    vento_max_kmh,
    dia_de_chuva,

    -- Médias e acumulados móveis de 7 dias
    round(temp_media_movel_7d_c, 1)  as temp_media_movel_7d_c,
    round(chuva_acumulada_7d_mm, 1)  as chuva_acumulada_7d_mm,
    dias_na_janela_7d,

    -- Qualidade
    dado_completo,
    temp_suspeita,

    -- Controle
    processado_em
from a_reprocessar
