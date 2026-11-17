# Documentación Técnica: Pipeline ETL & Data Warehouse de Riesgo Fiscal (SAT Art. 69-B y Padrón de Contribuyentes)

**Materia:** Software para la Toma de Decisiones  
**Autor:** Equipo de Ingeniería de Datos  
**Versión:** 2.0 (Producción / Local Standalone)  
**Entorno de Ejecución:** Python 3.10+ (Biblioteca Estándar Exclusiva) & SQLite 3  

---

## 1. Visión General y Objetivos del Sistema

El objetivo de esta solución es implementar un pipeline de datos robusto, auditable e idempotente para el análisis integral del riesgo fiscal en México. El sistema ingesta, diagnostica, estandariza y modela la información pública de:
1. **Listado de Contribuyentes del Artículo 69-B del Código Fiscal de la Federación (CFF):** Empresas que Facturan Operaciones Simuladas (EFOS), catalogadas en sus diferentes estatus procesales (*Presuntos*, *Desvirtuados*, *Definitivos* y *Sentencias Favorables*).
2. **Padrón de Contribuyentes (API CKAN datos.gob.mx):** Universo de contribuyentes inscritos en el Servicio de Administración Tributaria (SAT), desglosados históricamente por tipo de persona y estatus de actividad fiscal.

### Principio de Diseño "Cero Dependencias Externas"
Por políticas de seguridad, reproducibilidad y gobernanza institucional, el pipeline prescinde en su totalidad de paquetes externos (`pandas`, `numpy`, `requests`, `sqlalchemy`, etc.). Todas las rutinas de red, codificación, hashing criptográfico, procesamiento tabular y base de datos relacional se construyen sobre módulos nativos de Python (`urllib.request`, `json`, `csv`, `sqlite3`, `re`, `hashlib`, `datetime`, `pathlib`).

---

## 2. Arquitectura de Datos: Prototipo Local vs. Arquitectura Global Cloud

El diseño del proyecto sigue la arquitectura de capas **Medallion / Lambda reducida**, garantizando un mapeo 1:1 entre el entorno local (desarrollo, pruebas y toma de decisiones en desktop) y la infraestructura de producción distribuida en la nube.

```
Arquitectura Local (Prototipo Standalone):
┌────────────────┐      ┌────────────────┐      ┌────────────────┐      ┌────────────────┐
│   Capa RAW     │ ──►  │   Capa CLEAN   │ ──►  │    Capa DW     │ ──►  │   Capa MARTS   │
│ (raw/*.csv,    │      │ (clean/*.csv,  │      │ (dw/dw_sat.db, │      │ (marts/*.db,   │
│  manifest.json)│      │  rechazos/conf)│      │  raw_* espejo) │      │  dashboard.html│
└────────────────┘      └────────────────┘      └────────────────┘      └────────────────┘

Arquitectura Global en la Nube (Producción Cloud):
┌────────────────┐      ┌────────────────┐      ┌────────────────┐      ┌────────────────┐
│ Amazon S3 Raw  │ ──►  │ AWS Lambda /   │ ──►  │ Amazon RDS     │ ──►  │ Amazon         │
│ (Data Lake S3, │      │ AWS Glue /     │      │ PostgreSQL /   │      │ QuickSight /   │
│  Versionado)   │      │ MongoDB Atlas  │      │ Snowflake DW   │      │ BI Dashboard   │
└────────────────┘      └────────────────┘      └────────────────┘      └────────────────┘
```

### Tabla Comparativa de Alineación Arquitectónica

| Componente | Implementación Local (Este Proyecto) | Implementación Cloud Empresarial | Justificación Técnica |
|---|---|---|---|
| **Almacenamiento Crudo** | Directorio `raw/` con `manifest.json` | AWS S3 Bucket (Capa Bronze con S3 Object Lock) | Almacenamiento inmutable con trazabilidad criptográfica SHA-256. |
| **Orquestación ETL** | `main.py` con logging estructurado | AWS Step Functions o Apache Airflow | Ejecución dirigida por grafos de dependencias e idempotencia. |
| **Cómputo / Transformación**| Módulos Python nativos (`extract`, `profile`, `transform`) | AWS Lambda (Serverless) o AWS Glue Spark Jobs | Transformaciones deterministas sin dependencias pesadas. |
| **Zona Limpia / Curada** | `clean/` (CSVs normalizados y logs de rechazo) | AWS S3 Capa Silver / MongoDB Atlas (Documentos) | Separación estricta entre datos estructurados limpios y metadatos de auditoría. |
| **Motor Data Warehouse** | SQLite (`dw/dw_sat.db`) con `PRAGMA foreign_keys = ON` | Amazon RDS PostgreSQL / Snowflake | Motor ACID relacional para tablas espejo y modelo dimensional. |
| **Capa de Explotación** | `marts/` (SQLite) + `dashboard/` (HTML/CSS nativo) | Amazon QuickSight / Power BI / Tableau | Vistas analíticas y tableros interactivos para toma de decisiones ejecutivas. |

---

## 3. Justificación de Fuentes de Datos

La API abierta de `datos.gob.mx` expone más de 23 recursos asociados al Padrón de Contribuyentes y diversos cortes del SAT. La selección de este proyecto se optimizó bajo los siguientes criterios de negocio y valor analítico:

1. **Listado Completo Art. 69-B del CFF:**
   - *Valor Estratégico:* Fuente primaria de identificación de riesgo de evasión y operaciones simuladas.
   - *Particularidad:* Estructura ancha multi-evento que exige un despivoteo relacional a nivel atómico.
2. **Padrón de Contribuyentes: Activos por Tipo de Persona (Física vs. Moral):**
   - *Valor Estratégico:* Provee el denominador macroeconómico. Permite contrastar cuántas EFOS existen en relación con el universo real de personas morales y físicas en operación.
3. **Padrón de Contribuyentes: Por Situación ante el RFC (Activo, Suspendido, Cancelado):**
   - *Valor Estratégico:* Permite analizar si las empresas boletinadas permanecen en estatus activo previo a la publicación definitiva o si mudan rápidamente a esquemas suspendidos/cancelados.

---

## 4. Detalle Técnico de Módulos Implementados

### 4.1. Módulo de Extracción (`src/extract.py`)

#### Funcionalidades Principales
- **Interrogación CKAN API:** Envía peticiones HTTP GET estructuradas a `https://datos.gob.mx/api/3/action/package_show?id=padron` con `urllib.request`. Almacena la respuesta JSON cruda en `raw/metadatos_padron_YYYYMMDD.json`.
- **Filtrado Inteligente de Recursos:** Evalúa las listas de metadatos CKAN filtrando por palabras clave (`tipo de persona`, `situacion del contribuyente`) y descarga los binarios de los dos recursos prioritarios.
- **Resiliencia y Fallback de Red:** Si las URLs gubernamentales están fuera de línea, bloqueadas por firewall o sufren HTTP 403/500, el módulo:
  1. Busca si el operador manual colocó un archivo existente coincidente con `raw/*69b*.csv`.
  2. Si no existe, activa un generador determinista de datos de muestra para garantizar que el pipeline continúe su ejecución de extremo a extremo sin detenciones fatales.
- **Gobernanza Criptográfica:** Todo archivo descargado o generado se registra en `raw/manifest.json` con:
  - Ruta de archivo y nombre sanitizado.
  - Tamaño exacto en bytes (`st_size`).
  - Hash digital **SHA-256** para auditoría y no repudio.
  - URL origen o indicador de fallback.
- **Tablas Espejo en Data Warehouse:** Lee los CSV crudos utilizando el detector automático de codificación e inserta los datos íntegros con todas sus columnas como `TEXT` en tablas espejo de SQLite (`raw_69b`, `raw_padron_tipo`, `raw_padron_situacion`) dentro de `dw/dw_sat.db`.

---

### 4.2. Módulo de Perfilado (`src/profile.py`)

#### Funcionalidades Principales
- **Detección Automática de Codificación:** La función `detect_encoding(filepath)` lee los primeros 32,768 bytes del archivo para identificar firmas BOM UTF-8 (`\xef\xbb\xbf`) y evalúa en orden `utf-8-sig`, `utf-8`, `latin-1` y `cp1252`, evitando excepciones de tipo `UnicodeDecodeError`.
- **Detección de Delimitadores:** Evalúa frecuencias de separadores (`,`, `|`, `\t`) sobre muestras iniciales para archivos no convencionales.
- **Diagnóstico de Calidad y Completitud:**
  - Conteo de registros físicos totales (excluyendo encabezado).
  - Listado exacto de campos/columnas.
  - Conteo de valores nulos o cadenas en blanco y cálculo de porcentaje de completitud.
  - Visualización ASCII de nivel de pérdida por columna.
- **Detección Heurística de Formatos de Fecha:** Analiza patrones regex para identificar formatos en el texto:
  - Formato tradicional mexicano: `DD/MM/YYYY`
  - Formato estándar ISO: `YYYY-MM-DD`
  - Formatos textuales en español: `D de mes de YYYY` (ej. `15 de marzo de 2023`)
  - Formatos de periodo mensual: `YYYY-MM` y `YYYYMM`.
- **Salidas Generadas:**
  - `clean/perfilado_fuentes.md`: Reporte ejecutivo en formato Markdown.
  - `clean/perfilado_fuentes.json`: Ficha técnica de perfilado utilizable por herramientas automatizadas.

---

### 4.3. Módulo de Transformación (`src/transform.py`)

#### 4.3.1. Rama Listado 69-B del CFF

##### A. Limpieza y Validación de RFC
Se aplica la expresión regular oficial del SAT:
$$\text{RFC\_REGEX} = \texttt{\textasciicircum[A-ZÑ\&]\{3,4\}\textbackslash d\{6\}[A-Z0-9]\{3\}\$}$$
- **RFC Personas Morales:** 3 letras iniciales + 6 dígitos de fecha (AAMMDD) + 3 caracteres alfanuméricos de homoclave (longitud total: 12 caracteres).
- **RFC Personas Físicas:** 4 letras iniciales + 6 dígitos de fecha (AAMMDD) + 3 caracteres alfanuméricos de homoclave (longitud total: 13 caracteres).
- **Derivación de `tipo_persona`:**
  - Si $\text{longitud} = 12 \implies \text{Moral}$.
  - Si $\text{longitud} = 13 \implies \text{Física}$.
- **Trazabilidad Total de Rechazados:** Si un RFC no cumple con la regla sintáctica o está en blanco, la fila completa **no se elimina silenciosamente**. Se enruta a `clean/rechazados_69b.csv` registrando:
  `fila_origen`, `rfc`, `nombre_crudo`, `motivo_rechazo` (ej. `"RFC inválido"` o `"RFC ausente o vacío"`) y `datos_originales` en JSON serializado.

##### B. Tratamiento de Nombres y Campo `nota_sat`
En los listados del SAT, es común que funcionarios agreguen observaciones jurídicas dentro de la misma celda de la razón social mediante separadores `//` (ej. `"COMERCIALIZADORA EJEMPLO TRES SC // NOTA: Amparo en revisión 123/2023"`).
- Se aplica split por `//`.
- El texto a la izquierda se convierte a mayúsculas y se remueven espacios duplicados, poblando `nombre`.
- El texto a la derecha se normaliza y se aisla en el campo `nota_sat`.

##### C. Conversión Dinámica de Fechas en Español a ISO
Se procesan fechas heterogéneas mediante la función `parse_fecha_iso()`:
- `"15 de marzo de 2023"` $\implies$ `2023-03-15`.
- `"01/06/2023"` $\implies$ `2023-06-01`.
- Para los números de oficio que incluyen la fecha combinada (ej. `"500-05-2023-12345 de 15 de marzo de 2023"`), la función `parse_oficio_campo()` extrae el identificador del oficio (`"500-05-2023-12345"`) y convierte la fecha a ISO (`2023-03-15`).

##### D. Despivoteo (Ancho a Largo / Unpivot)
El archivo crudo del SAT organiza los datos de forma horizontal (ancho), colocando en columnas consecutivas las diferentes etapas del proceso de presunción y sus publicaciones en medios oficiales:
- **Etapas Procesales:** `Presunto`, `Desvirtuado`, `Definitivo`, `Sentencia Favorable`.
- **Medios de Difusión:** `SAT` (Portal web) y `DOF` (Diario Oficial de la Federación).

La función `detectar_columnas_etapas()` localiza dinámicamente las columnas correspondientes en los encabezados crudos sin depender de posiciones fijas. Por cada combinación que contenga evidencia de publicación u oficio, se emite un registro atómico en `clean/publicaciones_long.csv`:

$$\text{Fila Atómica} = (\texttt{rfc}, \texttt{situacion\_69b}, \texttt{medio}, \texttt{numero\_oficio}, \texttt{fecha\_oficio}, \texttt{fecha\_publicacion})$$

##### E. Detección de Conflictos de Razón Social
Si durante la lectura del dataset un mismo RFC válido reaparece con una denominación o razón social distinta a la registrada previamente (por ejemplo, por modificaciones estatutarias, errores de captura del SAT o cambios de régimen), el registro se documenta formalmente en `clean/conflictos_rfc.csv` para auditoría jurídica y fiscal.

---

#### 4.3.2. Rama Padrón de Contribuyentes

El archivo `clean/padron_universo_limpio.csv` consolida los datos macro del padrón nacional:
1. **Normalización de Periodos a Fecha de Corte (`fecha_corte`):**
   - Entradas como `"2023-01"`, `"2023/04"`, `"202307"` se transforman en una fecha de corte estándar al primer día del mes: `AAAA-MM-01` (ej. `2023-01-01`).
   - Se derivan analíticamente las columnas enteras `anio`, `mes` y `trimestre` (donde $\text{trimestre} = \lfloor(\text{mes}-1)/3\rfloor + 1$).
2. **Homologación Categórica:**
   - Tipo de persona: se homologa a valores únicos `Física` o `Moral`.
   - Estatus ante el RFC: se estandariza a `Activo`, `Suspendido` o `Cancelado`.
3. **Limpieza Numérica:**
   - Cadenas con formato de millares (ej. `"78,452,190"`) se limpian y convierten a enteros limpios (`78452190`).

---

#### 4.3.3. Cuadro de Balance y Métrica de Transformación
Toda la conciliación numérica se serializa en `clean/resumen_transformacion.json` y se muestra en la terminal:

```json
{
  "pipeline_run_ts": "2026-11-17T01:52:17+00:00",
  "rama_69b": {
    "archivo_origen": "listado_69b_raw.csv",
    "filas_leidas": 8,
    "filas_validas": 6,
    "filas_rechazadas": 1,
    "motivos_rechazo": {
      "RFC inválido": 1
    },
    "conflictos_rfc_distintos_nombres": 1,
    "filas_despivoteadas_formato_largo": 22
  },
  "rama_padron": {
    "archivos_procesados": 2,
    "filas_generadas": 25
  }
}
```

---

## 5. Instrucciones de Ejecución y Auditoría

### 5.1. Requisitos Previos
- Python 3.10 o superior instalado.
- Terminal o consola de comandos (PowerShell, Bash o CMD).
- No se requiere conexión a internet permanente (el sistema cuenta con fallbacks integrados).

### 5.2. Comandos de Ejecución

```bash
# Navegar al directorio del proyecto
cd "etl-sat-69b"

# 1. Ejecutar únicamente las 3 fases solicitadas (Extracción, Perfilado, Transformación)
python main.py --extract --profile --transform

# 2. Ejecutar individualmente una sola fase
python main.py --extract
python main.py --profile
python main.py --transform

# 3. Ejecutar el pipeline completo de extremo a extremo (incluyendo DW, Marts y Dashboard)
python main.py

# 4. Consultar menú de ayuda
python main.py --help
```

---

## 6. Procedimiento de Auditoría de Resultados

Para certificar la integridad del proceso, el auditor o evaluador debe verificar los siguientes artefactos:

| Capa | Archivo de Auditoría | Criterio de Aprobación |
|---|---|---|
| **RAW** | `raw/manifest.json` | Contiene hashes SHA-256 de todos los archivos descargados o generados. |
| **RAW** | `dw/dw_sat.db` | Tablas `raw_69b`, `raw_padron_tipo` y `raw_padron_situacion` creadas con columnas intactas `TEXT`. |
| **PERFILADO** | `clean/perfilado_fuentes.md` | Diagnóstico de nulos por columna, codificación y formatos de fecha detectados. |
| **CLEAN (69-B)** | `clean/contribuyentes_limpios.csv` | RFC válidos, tipo persona asignada, nombres limpios y campo `nota_sat` aislado. |
| **CLEAN (69-B)** | `clean/publicaciones_long.csv` | Estructura vertical con etapas procesales y medios (`SAT`/`DOF`) normalizados a fechas ISO. |
| **CLEAN (69-B)** | `clean/rechazados_69b.csv` | Cada fila rechazada cuenta con motivo formal explícito (`RFC inválido`, etc.). |
| **CLEAN (69-B)** | `clean/conflictos_rfc.csv` | Registra diferencias de razón social detectadas para un mismo RFC. |
| **CLEAN (PADRÓN)** | `clean/padron_universo_limpio.csv` | Periodos normalizados a `AAAA-MM-01`, enteros limpios sin comas y categorías homologadas. |
| **AUDITORÍA** | `clean/resumen_transformacion.json` | Balance cuantitativo: $\text{filas leídas} = \text{válidas} + \text{rechazadas}$. |
