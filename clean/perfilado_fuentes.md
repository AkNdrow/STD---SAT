# Reporte de Perfilado de Fuentes de Datos

> Generado automáticamente: `2026-11-17T01:54:11+00:00`

**Archivos analizados:** 3

## listado_69b_raw.csv

| Propiedad | Valor |
|-----------|-------|
| Codificación | `utf-8-sig` |
| Total de filas | 8 |
| Total de columnas | 16 |

### Análisis de completitud por columna

| # | Columna | Nulos | No nulos | % Nulo |
|---|---------|-------|----------|--------|
| 0 | `No` | 0 | 8 | 0.0% |
| 1 | `RFC` | 0 | 8 | 0.0% |
| 2 | `Nombre del Contribuyente` | 0 | 8 | 0.0% |
| 3 | `Situación del contribuyente` | 0 | 8 | 0.0% |
| 4 | `Número y fecha de oficio global de presunción` | 0 | 8 | 0.0% |
| 5 | `Publicación página SAT presuntos` | 0 | 8 | 0.0% |
| 6 | `Publicación DOF presuntos` | 0 | 8 | 0.0% |
| 7 | `Número y fecha de oficio global de contribuyentes que desvirtuaron` | 7 | 1 | 87.5% |
| 8 | `Publicación página SAT desvirtuados` | 7 | 1 | 87.5% |
| 9 | `Publicación DOF desvirtuados` | 3 | 5 | 37.5% |
| 10 | `Número y fecha de oficio global de definitivos` | 4 | 4 | 50.0% |
| 11 | `Publicación página SAT definitivos` | 4 | 4 | 50.0% |
| 12 | `Publicación DOF definitivos` | 7 | 1 | 87.5% |
| 13 | `Número y fecha de oficio global de sentencia favorable` | 7 | 1 | 87.5% |
| 14 | `Publicación página SAT sentencia favorable` | 7 | 1 | 87.5% |
| 15 | `Publicación DOF sentencia favorable` | 8 | 0 | 100.0% |

### Formatos de fecha detectados

| Formato | Ejemplos |
|---------|----------|
| `DD/MM/YYYY` | `01/06/2023`, `15/06/2023`, `15/10/2023` |
| `D de mes de YYYY` | `15 de marzo de 2023`, `1 de septiembre de 2023`, `10 de junio de 2022` |

---

## padron_contribuyentes_tipo_persona_20261116.csv

| Propiedad | Valor |
|-----------|-------|
| Codificación | `utf-8-sig` |
| Total de filas | 10 |
| Total de columnas | 3 |

### Análisis de completitud por columna

| # | Columna | Nulos | No nulos | % Nulo |
|---|---------|-------|----------|--------|
| 0 | `periodo` | 0 | 10 | 0.0% |
| 1 | `tipo_persona` | 0 | 10 | 0.0% |
| 2 | `total_contribuyentes` | 0 | 10 | 0.0% |

### Formatos de fecha detectados

| Formato | Ejemplos |
|---------|----------|
| `YYYY-MM` | `2023-01`, `2023-04`, `2023-07` |

---

## padron_situacion_contribuyente_20261116.csv

| Propiedad | Valor |
|-----------|-------|
| Codificación | `utf-8-sig` |
| Total de filas | 15 |
| Total de columnas | 3 |

### Análisis de completitud por columna

| # | Columna | Nulos | No nulos | % Nulo |
|---|---------|-------|----------|--------|
| 0 | `periodo` | 0 | 15 | 0.0% |
| 1 | `estatus_rfc` | 0 | 15 | 0.0% |
| 2 | `total_contribuyentes` | 0 | 15 | 0.0% |

### Formatos de fecha detectados

| Formato | Ejemplos |
|---------|----------|
| `YYYY-MM` | `2023-01`, `2023-04`, `2023-07` |

---
