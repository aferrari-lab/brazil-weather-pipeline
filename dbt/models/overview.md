{% docs __overview__ %}
# brazil-weather-pipeline: documentação do dbt

Aqui eu documento a camada de transformação do projeto: o clima diário de todos os
municípios brasileiros desde 01/01/2025, com dados da Open-Meteo (CC BY 4.0) e do IBGE.

## De onde vêm os dados

```
Open-Meteo + IBGE → Bronze (Parquet no lake) → Silver (PySpark) → Postgres (schema silver) → dbt → Gold
```

O dbt começa onde o Spark termina: ele lê as tabelas `silver.clima_diario` e
`silver.municipios` do Postgres (as **sources**) e constrói as camadas abaixo.

## Camadas

| Camada | Schema | Materialização | Modelos |
| --- | --- | --- | --- |
| Staging | `staging` | view | `stg_clima_diario`, `stg_municipios`: nomes e tipos padronizados, sem regra de negócio |
| Intermediate | `intermediate` | view | `int_clima_metricas`: média móvel e chuva acumulada de 7 dias corridos |
| Gold | `gold` | tabela | `dim_municipio`, `dim_data`, `fct_clima_diario` (incremental), `agg_clima_mensal`, `fct_eventos_climaticos` e `agg_eventos_mensal` (ondas de calor e de frio, chuva extrema, estiagem) |
| Observabilidade | `observabilidade` | tabela | `obs_cobertura_diaria`, `obs_cobertura_uf`, `obs_volume_particao`, `obs_cargas`, `obs_resultados_testes`: a saúde do próprio pipeline |

## Como eu garanto a qualidade

- **Testes de dados** em todas as camadas: chaves únicas e não nulas, relacionamentos
  entre fato e dimensões, faixas plausíveis (testes genéricos próprios `combinacao_unica`
  e `entre`) e um teste que confere se o fato tem todas as linhas da staging.
- **Freshness** da source: aviso se a silver ficar 2 dias sem atualizar, erro com 7.
- **Falhas gravadas** no schema `testes_falhas`, para investigar as linhas no banco.
- **Histórico de execuções:** cada `dbt build` grava o resultado de cada modelo e teste em `observabilidade.dbt_resultados`.
- **Alerta de volume:** todo mês fechado precisa ter exatamente municípios × dias linhas por UF.

## Decisões que vale conhecer

- `fct_clima_diario` é **incremental** (delete+insert por `codigo_ibge` e `data`). Quando um
  dia muda, eu reprocesso também os 6 dias seguintes, porque as métricas móveis de 7 dias
  deles dependem desse dia. Assim o incremental dá exatamente o mesmo resultado do full-refresh.
- As janelas móveis usam `RANGE` de 7 dias **corridos**, e não 7 linhas: se faltar um dia,
  a média não "pula" para trás.
- `agg_clima_mensal` traz a **anomalia** de temperatura: a média do mês menos a média do mesmo
  mês no histórico do município. Com o histórico ainda curto, a coluna
  `anos_na_media_historica` mostra quantos anos entraram na conta.

## Como navegar

Use a aba **Project** à esquerda para ver os modelos por pasta, ou o botão de **linhagem**
(canto inferior direito) para ver o grafo de dependências das sources até a gold.
{% enddocs %}
