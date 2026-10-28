# Benchmark: pandas × PySpark

Executado em 28/10/2026, Spark local com 16 núcleo(s), mediana de 3 execuções. Dados sintéticos no formato do bronze real (2% de linhas repetidas para a deduplicação).

| Linhas no bronze | Cenário | pandas (s) | Spark (s) | pandas ÷ Spark |
| ---: | --- | ---: | ---: | ---: |
| 102.000 | ponta_a_ponta | 0.14 | 1.81 | 0.08× |
| 102.000 | busca_sem_particoes | 0.01 | 0.14 | 0.08× |
| 102.000 | busca_com_particoes | 0.01 | 0.13 | 0.05× |
| 1.020.000 | ponta_a_ponta | 0.90 | 9.11 | 0.10× |
| 1.020.000 | busca_sem_particoes | 0.01 | 0.11 | 0.13× |
| 1.020.000 | busca_com_particoes | 0.01 | 0.14 | 0.09× |
| 3.060.000 | ponta_a_ponta | 3.57 | 25.61 | 0.14× |
| 3.060.000 | busca_sem_particoes | 0.02 | 0.09 | 0.23× |
| 3.060.000 | busca_com_particoes | 0.03 | 0.16 | 0.17× |

**Ganho do particionamento** (buscar SP em janeiro de 2025):

| Linhas no bronze | Ferramenta | Sem partições (s) | Com partições (s) | Ganho |
| ---: | --- | ---: | ---: | ---: |
| 102.000 | pandas | 0.01 | 0.01 | 1.6× |
| 102.000 | spark | 0.14 | 0.13 | 1.1× |
| 1.020.000 | pandas | 0.01 | 0.01 | 1.1× |
| 1.020.000 | spark | 0.11 | 0.14 | 0.8× |
| 3.060.000 | pandas | 0.02 | 0.03 | 0.8× |
| 3.060.000 | spark | 0.09 | 0.16 | 0.6× |

Valores acima de 1× em "pandas ÷ Spark" significam que o Spark foi mais rápido.

```mermaid
xychart-beta
    title "Ponta a ponta (ler, transformar, gravar), em segundos"
    x-axis ["102k", "1.020k", "3.060k"]
    y-axis "segundos" 0 --> 29
    bar [0.14, 0.90, 3.57]
    line [1.81, 9.11, 25.61]
```

Barras: pandas. Linha: Spark.
