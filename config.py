"""
config.py — Configuración centralizada del pipeline ETL SAT 69-B
================================================================
Todas las rutas, URLs y parámetros del proyecto se definen aquí.
No se usan librerías externas: solo pathlib y módulos estándar.
"""

from pathlib import Path
from datetime import datetime, timezone

# ──────────────────────────────────────────────
# Rutas del proyecto
# ──────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent

RAW_DIR    = BASE_DIR / "raw"
CLEAN_DIR  = BASE_DIR / "clean"
DW_DIR     = BASE_DIR / "dw"
MARTS_DIR  = BASE_DIR / "marts"
DASH_DIR   = BASE_DIR / "dashboard"
DOCS_DIR   = BASE_DIR / "docs"

# Crear directorios si no existen
for _d in (RAW_DIR, CLEAN_DIR, DW_DIR, MARTS_DIR, DASH_DIR, DOCS_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# ──────────────────────────────────────────────
# Base de datos del Data Warehouse
# ──────────────────────────────────────────────
DW_DB_PATH   = DW_DIR / "dw_sat.db"
MARTS_DB_PATH = MARTS_DIR / "sat_marts.db"

# ──────────────────────────────────────────────
# Fuentes de datos — API CKAN datos.gob.mx
# ──────────────────────────────────────────────
CKAN_PACKAGE_URL = "https://datos.gob.mx/api/3/action/package_show?id=padron"

# Listado 69-B — URL directa (puede cambiar; fallback a archivo local)
URL_69B = (
    "https://datos.gob.mx/busca/dataset/e9e6e554-4599-4aab-9e17-6e425e42348a/"
    "resource/4e36a9c4-72d7-4bd3-8197-4bee00dead7f/download/"
    "lista_completa_69-b_2024.csv"
)

# Nombres de recursos prioritarios del Padrón (substrings para match)
PADRON_PRIORITY_KEYWORDS = [
    "tipo de persona",          # Contribuyentes activos por tipo de persona
    "situacion del contribuyente",  # Contribuyentes por situación ante el RFC
]

# ──────────────────────────────────────────────
# Parámetros de extracción
# ──────────────────────────────────────────────
HTTP_TIMEOUT_SECS = 90
HTTP_USER_AGENT = "ETL-SAT-69B/2.0 (Proyecto Academico STD-UNAM)"
HASH_ALGO = "sha256"

# ──────────────────────────────────────────────
# Parámetros del pipeline
# ──────────────────────────────────────────────
PIPELINE_RUN_TS = datetime.now(timezone.utc).isoformat(timespec="seconds")
TODAY_STAMP = datetime.now().strftime("%Y%m%d")

# ──────────────────────────────────────────────
# Codificaciones a intentar (en orden de prioridad)
# ──────────────────────────────────────────────
ENCODING_CANDIDATES = ["utf-8-sig", "utf-8", "latin-1", "cp1252"]

# ──────────────────────────────────────────────
# Regex — RFC mexicano
# 3-4 letras (incluye Ñ y &) + 6 dígitos (AAMMDD) + 3 alfanuméricos
# ──────────────────────────────────────────────
RFC_REGEX = r"^[A-ZÑ&]{3,4}\d{6}[A-Z0-9]{3}$"

# ──────────────────────────────────────────────
# Meses en español para parseo de fechas textuales
# ──────────────────────────────────────────────
MESES_ES = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
    "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
    "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
}

# ──────────────────────────────────────────────
# Catálogo de situaciones 69-B
# ──────────────────────────────────────────────
SITUACIONES_CATALOGO = {
    "Presunto":              1,
    "Definitivo":            2,
    "Sentencia Favorable":   3,
    "Desvirtuado":           4,
}

# ──────────────────────────────────────────────
# Catálogo de homologación — Padrón
# ──────────────────────────────────────────────
HOMOLOGA_TIPO_PERSONA = {
    "fisica": "Física", "física": "Física", "f": "Física",
    "moral": "Moral", "m": "Moral",
}
HOMOLOGA_ESTATUS_RFC = {
    "activo": "Activo", "a": "Activo",
    "suspendido": "Suspendido", "s": "Suspendido",
    "cancelado": "Cancelado", "c": "Cancelado",
    "cancelación": "Cancelado",
}

# ──────────────────────────────────────────────
# Columnas esperadas en el CSV del 69-B
# ──────────────────────────────────────────────
EXPECTED_COLUMNS_69B = [
    "no",
    "rfc",
    "nombre_contribuyente",
    "situacion",
    "numero_fecha_oficio_global_presuncion",
    "publicacion_pagina_sat_presuntos",
    "numero_fecha_oficio_global_presuncion_desvirtuados",
    "publicacion_pagina_sat_desvirtuados",
    "numero_fecha_oficio_global_definitivos",
    "publicacion_pagina_sat_definitivos",
    "numero_fecha_oficio_global_sentencia_favorable",
    "publicacion_pagina_sat_sentencia_favorable",
]

# ──────────────────────────────────────────────
# Datasets y Mapeos del Pipeline
# ──────────────────────────────────────────────
DATASETS = {
    "listado_69b": {
        "raw_file": RAW_DIR / "listado_69b_raw.csv",
        "clean_file": CLEAN_DIR / "contribuyentes_limpios.csv",
        "clean_long": CLEAN_DIR / "publicaciones_long.csv",
        "description": "Listado Art. 69-B CFF — EFOS presuntas y definitivas",
    },
    "padron": {
        "clean_file": CLEAN_DIR / "padron_universo_limpio.csv",
        "description": "Padrón de Contribuyentes del SAT normalizado",
    },
}
LOG_FILE = BASE_DIR / "pipeline.log"
LOG_LEVEL = "INFO"
