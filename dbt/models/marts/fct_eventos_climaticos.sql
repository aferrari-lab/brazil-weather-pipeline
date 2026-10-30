-- Classificação de cada dia de cada município em eventos climáticos.
--
-- Critérios (inspirados nos índices de extremos do ETCCDI/OMM, com percentis do
-- próprio município no mesmo mês, ver int_limiares_clima):
-- - dia quente: máxima acima do percentil 90 (TX90p);
-- - dia frio: mínima abaixo do percentil 10 (TN10p);
-- - onda de calor / de frio: var('dias_minimos_onda') ou mais dias quentes / frios
--   seguidos. O índice WSDI do ETCCDI usa 6 dias; uso 3 porque, com histórico
--   curto e clima tropical, 6 dias seguidos acima do percentil 90 quase não
--   acontecem, e o objetivo aqui é um painel que mostre episódios relevantes;
-- - chuva extrema: chuva acima do percentil 95 dos dias chuvosos (R95p) e de um
--   mínimo absoluto, para um mês seco não transformar 3 mm em "extremo";
-- - estiagem: o dia em que a sequência de dias secos chega a
--   var('dias_minimos_estiagem') ou mais (base do índice CDD).
--
-- Cada dia ganha as flags de todos os critérios e um evento_principal, pela
-- prioridade da lista acima, para o painel mostrar uma categoria por dia.
-- É uma classificação por regra, transparente e testável: não precisa de modelo.

with base as (

    select
        f.codigo_ibge,
        f.data,
        f.temp_max_c,
        f.temp_min_c,
        f.chuva_mm,
        l.limiar_temp_max_c,
        l.limiar_temp_min_c,
        l.limiar_chuva_mm,
        l.limiar_confiavel,
        f.temp_max_c > l.limiar_temp_max_c as dia_quente,
        f.temp_min_c < l.limiar_temp_min_c as dia_frio,
        coalesce(f.chuva_mm, 0) < {{ var('limite_dia_chuva_mm') }} as dia_seco,
        coalesce(
            f.chuva_mm >= l.limiar_chuva_mm
            and f.chuva_mm >= {{ var('chuva_minima_extrema_mm') }},
            false
        ) as chuva_extrema
    from {{ ref('fct_clima_diario') }} as f
    inner join {{ ref('int_limiares_clima') }} as l
        on
            f.codigo_ibge = l.codigo_ibge
            and extract(month from f.data)::int = l.mes

),

-- Sequências de dias seguidos ("gaps and islands"): em dias consecutivos, a data
-- menos a posição na lista dá sempre o mesmo valor, que identifica a sequência.
quentes as (

    select
        codigo_ibge,
        data,
        data - (row_number() over (partition by codigo_ibge order by data))::int as grupo
    from base
    where dia_quente

),

frios as (

    select
        codigo_ibge,
        data,
        data - (row_number() over (partition by codigo_ibge order by data))::int as grupo
    from base
    where dia_frio

),

secos as (

    select
        codigo_ibge,
        data,
        data - (row_number() over (partition by codigo_ibge order by data))::int as grupo
    from base
    where dia_seco

),

tamanho_quentes as (

    select
        codigo_ibge,
        data,
        count(*) over (partition by codigo_ibge, grupo) as dias_seguidos
    from quentes

),

tamanho_frios as (

    select
        codigo_ibge,
        data,
        count(*) over (partition by codigo_ibge, grupo) as dias_seguidos
    from frios

),

-- Na seca conto os dias até aqui (e não o tamanho total da sequência): o dia
-- só entra em estiagem quando ela já dura o mínimo, sem olhar para o futuro.
posicao_secos as (

    select
        codigo_ibge,
        data,
        row_number() over (partition by codigo_ibge, grupo order by data) as dias_secos_seguidos
    from secos

),

classificado as (

    select
        b.*,
        coalesce(tq.dias_seguidos >= {{ var('dias_minimos_onda') }}, false) as onda_de_calor,
        coalesce(tf.dias_seguidos >= {{ var('dias_minimos_onda') }}, false) as onda_de_frio,
        coalesce(ps.dias_secos_seguidos, 0) as dias_secos_seguidos,
        coalesce(ps.dias_secos_seguidos >= {{ var('dias_minimos_estiagem') }}, false)
            as estiagem
    from base as b
    left join tamanho_quentes as tq on b.codigo_ibge = tq.codigo_ibge and b.data = tq.data
    left join tamanho_frios as tf on b.codigo_ibge = tf.codigo_ibge and b.data = tf.data
    left join posicao_secos as ps on b.codigo_ibge = ps.codigo_ibge and b.data = ps.data

)

select
    codigo_ibge,
    data,
    temp_max_c,
    temp_min_c,
    chuva_mm,
    coalesce(dia_quente, false) as dia_quente,
    coalesce(dia_frio, false) as dia_frio,
    onda_de_calor,
    onda_de_frio,
    chuva_extrema,
    estiagem,
    dias_secos_seguidos,
    case
        when onda_de_calor then 'onda de calor'
        when onda_de_frio then 'onda de frio'
        when chuva_extrema then 'chuva extrema'
        when estiagem then 'estiagem'
        when dia_quente then 'dia quente'
        when dia_frio then 'dia frio'
        else 'normal'
    end as evento_principal,
    round(limiar_temp_max_c::numeric, 1) as limiar_temp_max_c,
    round(limiar_temp_min_c::numeric, 1) as limiar_temp_min_c,
    round(limiar_chuva_mm::numeric, 1) as limiar_chuva_mm,
    limiar_confiavel
from classificado
