# 📊 ETL SAT 69-B — Pipeline de Riesgo Fiscal

> **Materia:** Software para la Toma de Decisiones  
> **Enfoque:** Análisis de riesgo fiscal con datos abiertos del SAT  
> **Motor:** Python 3.10+ (solo biblioteca estándar) + SQLite

---

## 🏗️ Arquitectura del Proyecto

```text
etl-sat-69b/
├── .gitignore              # Exclusiones de Git
├── README.md               # Este archivo
├── config.py               # Configuración centralizada
├── main.py                 # Orquestador del pipeline
├── raw/                    # Datos crudos descargados
├── clean/                  # Datos limpios + reportes de perfilado
├── dw/                     # Data Warehouse (SQLite, esquema estrella)
├── marts/                  # Data Marts analíticos (SQLite)
├── dashboard/              # Dashboard HTML estático
└── src/
    ├── __init__.py
    ├── extract.py           # Fase 1: Extracción desde datos.gob.mx
    ├── profile.py           # Fase 2: Perfilado de calidad de datos
    ├── transform.py         # Fase 3: Limpieza y normalización
    ├── load_dw.py           # Fase 4: Carga al Data Warehouse
    ├── build_marts.py       # Fase 5: Construcción de Data Marts
    └── generate_dashboard.py # Fase 6: Generación de dashboard
```

## 🔄 Flujo del Pipeline (ETL)

```
┌─────────────┐   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐
│  1.EXTRACT  │──►│  2.PROFILE  │──►│ 3.TRANSFORM │──►│  4.LOAD DW  │──►│  5.MARTS    │──►│ 6.DASHBOARD │
│  datos.gob  │   │  calidad    │   │  limpieza   │   │  esquema ★  │   │  analíticos │   │  HTML       │
│  → raw/     │   │  → clean/   │   │  → clean/   │   │  → dw/      │   │  → marts/   │   │  → dash/    │
└─────────────┘   └─────────────┘   └─────────────┘   └─────────────┘   └─────────────┘   └─────────────┘
```

## 🚀 Uso Rápido

```bash
# Pipeline completo (6 fases)
python main.py

# Fases individuales
python main.py extract          # Solo descarga
python main.py transform load   # Transformar y cargar al DW
python main.py marts dashboard  # Solo marts y dashboard

# Ayuda
python main.py --help
```

## 📐 Esquema Estrella (Data Warehouse)

```
              ┌──────────────────┐
              │  dim_situacion   │
              │──────────────────│
              │ situacion_id  PK │
              │ situacion_nombre │
              │ descripcion      │
              │ nivel_riesgo     │
              └────────┬─────────┘
                       │
┌──────────────────┐   │   ┌──────────────────┐
│ dim_contribuyente│   │   │   dim_tiempo      │
│──────────────────│   │   │──────────────────│
│ contribuyente_id │   │   │ fecha_id      PK │
│ rfc              │   │   │ anio, mes, dia   │
│ nombre           │   │   │ trimestre        │
│ tipo_persona     │   │   │ nombre_mes       │
│ longitud_rfc     │   │   │ dia_semana       │
└────────┬─────────┘   │   └────────┬─────────┘
         │             │            │
         └─────────────┼────────────┘
                       │
              ┌────────▼─────────┐
              │ fact_listado_69b │
              │──────────────────│
              │ fact_id       PK │
              │ row_hash      UK │
              │ contribuyente_id │◄── FK
              │ situacion_id     │◄── FK
              │ fecha_presuncion │◄── FK
              │ fecha_definitivo │◄── FK
              │ fecha_sentencia  │◄── FK
              │ num_oficio_*     │
              │ pipeline_ts      │
              └──────────────────┘
```

## 📊 Data Marts Generados

| Mart                      | Descripción                                      |
|---------------------------|--------------------------------------------------|
| `mart_riesgo_fiscal`      | Score de riesgo (1-100) por contribuyente         |
| `mart_por_situacion`      | Concentrado por tipo de situación 69-B            |
| `mart_tendencia_temporal` | Tendencia mensual/anual de publicaciones          |
| `mart_tipo_persona`       | Distribución persona física vs. moral             |

## ⚠️ Restricciones Técnicas

- **Cero dependencias externas**: No se usa `pandas`, `numpy`, `requests` ni ningún paquete de `pip`
- **Solo biblioteca estándar**: `csv`, `sqlite3`, `urllib.request`, `json`, `re`, `hashlib`, `datetime`, `pathlib`
- **Base de datos**: SQLite con `PRAGMA foreign_keys = ON`
- **Idempotente**: Cada ejecución recrea las capas desde cero (recarga completa)
- **Sin red disponible**: El extractor genera datos de ejemplo automáticamente si no hay conectividad

## 📂 Fuente de Datos

- **SAT — Listado Art. 69-B del CFF**: Contribuyentes con operaciones presuntamente inexistentes (EFOS)
- **Portal**: [datos.gob.mx](https://datos.gob.mx)
- **API**: `https://api.datos.gob.mx/v1/`

## 🏷️ Catálogo de Situaciones 69-B

| Situación             | ID | Nivel Riesgo | Descripción                                       |
|-----------------------|----|--------------|---------------------------------------------------|
| Presunto              |  1 | ALTO         | Presunción de operaciones inexistentes             |
| Definitivo            |  2 | ALTO         | Determinación definitiva de EFOS                   |
| Sentencia Favorable   |  3 | BAJO         | Sentencia favorable del tribunal                   |
| Desvirtuado           |  4 | NULO         | Presunción desvirtuada ante el SAT                 |
