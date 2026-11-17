"""
src/extract.py — Fase EXTRACT del pipeline ETL SAT 69-B
========================================================
Descarga los datos crudos desde la API CKAN de datos.gob.mx
y los almacena en raw/ como archivos CSV/JSON.

Responsabilidades:
  1. Consultar la API CKAN package_show para obtener metadatos del Padrón.
  2. Filtrar los 2 recursos de prioridad alta y descargarlos en raw/.
  3. Descargar el Listado 69-B (con fallback a archivo local o muestra).
  4. Calcular SHA-256 y registrar manifest.json.
  5. Cargar CSVs crudos como tablas espejo (all-TEXT) en dw/dw_sat.db.

Solo biblioteca estándar: urllib.request, json, csv, hashlib, sqlite3.
"""

import csv
import hashlib
import json
import logging
import sqlite3
import ssl
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

import config


# ──────────────────────────────────────────────
# Utilidades HTTP y Criptográficas
# ──────────────────────────────────────────────

def _ssl_context() -> ssl.SSLContext:
    """Contexto SSL seguro para endpoints gubernamentales."""
    return ssl.create_default_context()


def _http_get(url: str, logger: logging.Logger, accept: str = "*/*") -> bytes:
    """GET simple con User-Agent y timeout. Retorna bytes crudos."""
    logger.debug("  GET %s", url)
    req = urllib.request.Request(url, headers={
        "User-Agent": config.HTTP_USER_AGENT,
        "Accept": accept,
    })
    with urllib.request.urlopen(
        req, timeout=config.HTTP_TIMEOUT_SECS, context=_ssl_context()
    ) as resp:
        logger.info("    HTTP %d — Content-Length: %s",
                    resp.status, resp.headers.get("Content-Length", "?"))
        return resp.read()


def _sha256(filepath: Path) -> str:
    """Calcula el hash SHA-256 de un archivo en bloques de 64KB."""
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(65_536), b""):
            h.update(chunk)
    return h.hexdigest()


# ──────────────────────────────────────────────
# Detección de codificación automática
# ──────────────────────────────────────────────

def detect_encoding(filepath: Path) -> str:
    """
    Detecta automáticamente la codificación leyendo los primeros 32KB:
      utf-8-sig → utf-8 → latin-1 → cp1252
    Garantiza compatibilidad sin lanzar UnicodeDecodeError.
    """
    raw = filepath.read_bytes()[:32_768]

    # BOM UTF-8
    if raw[:3] == b"\xef\xbb\xbf":
        return "utf-8-sig"

    for enc in config.ENCODING_CANDIDATES:
        try:
            raw.decode(enc)
            return enc
        except (UnicodeDecodeError, LookupError):
            continue

    return "latin-1"  # Fallback infalible


# ──────────────────────────────────────────────
# 1. Consultar API CKAN — Metadatos del Padrón
# ──────────────────────────────────────────────

def _fetch_ckan_metadata(logger: logging.Logger) -> dict:
    """
    Descarga metadatos desde la API CKAN del Padrón y guarda
    en raw/metadatos_padron_YYYYMMDD.json.
    """
    out_path = config.RAW_DIR / f"metadatos_padron_{config.TODAY_STAMP}.json"
    logger.info("  Consultando API CKAN: %s", config.CKAN_PACKAGE_URL)

    try:
        raw_bytes = _http_get(config.CKAN_PACKAGE_URL, logger, accept="application/json")
        data = json.loads(raw_bytes.decode("utf-8"))
        out_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info("  ✔ Metadatos guardados: %s", out_path.name)
        return data
    except Exception as exc:
        logger.warning("  No fue posible consultar API CKAN en línea (%s)", exc)
        existing = sorted(config.RAW_DIR.glob("metadatos_padron_*.json"), reverse=True)
        if existing:
            logger.info("  Cargando metadatos existentes locales: %s", existing[0].name)
            return json.loads(existing[0].read_text(encoding="utf-8"))
        return {}


# ──────────────────────────────────────────────
# 2. Filtrar y descargar recursos prioritarios del Padrón
# ──────────────────────────────────────────────

def _filter_priority_resources(ckan_data: dict, logger: logging.Logger) -> list[dict]:
    """Filtra los 2 recursos de alta prioridad en base a palabras clave."""
    result_obj = ckan_data.get("result", ckan_data)
    resources = result_obj.get("resources", [])
    if not resources:
        return []

    priority = []
    for res in resources:
        text = (res.get("name", "") + " " + res.get("description", "")).lower()
        for kw in config.PADRON_PRIORITY_KEYWORDS:
            if kw.lower() in text:
                priority.append(res)
                logger.info("    ✔ Recurso prioritario encontrado: %s", res.get("name", res.get("id")))
                break
        if len(priority) >= 2:
            break
    return priority


def _download_padron_resource(res: dict, idx: int, logger: logging.Logger) -> dict[str, Any] | None:
    """Descarga un recurso individual del padrón."""
    url = res.get("url", "")
    name = res.get("name", f"recurso_{idx}")
    fmt = res.get("format", "csv").lower()
    safe_name = "".join(c if c.isalnum() or c in "-_ " else "_" for c in name).strip().replace(" ", "_")[:50]
    out_path = config.RAW_DIR / f"padron_{safe_name}_{config.TODAY_STAMP}.{fmt}"

    if not url:
        return None

    try:
        raw_bytes = _http_get(url, logger)
        out_path.write_bytes(raw_bytes)
        sha = _sha256(out_path)
        logger.info("    ✔ Guardado: %s (%d bytes)", out_path.name, out_path.stat().st_size)
        return {
            "file": str(out_path),
            "filename": out_path.name,
            "source_url": url,
            "size_bytes": out_path.stat().st_size,
            "sha256": sha,
            "resource_name": name,
        }
    except Exception as exc:
        logger.warning("    No se pudo descargar recurso '%s': %s", name, exc)
        return None


def _generate_sample_padron(logger: logging.Logger) -> list[dict[str, Any]]:
    """
    Genera datasets de muestra para los 2 recursos prioritarios del Padrón
    (tipo de persona y situación ante el RFC) si la API gubernamental no responde.
    """
    logger.info("  Generando datasets representativos del Padrón (fallback)...")
    res_list = []

    # Recurso 1: Tipo de persona
    path_tipo = config.RAW_DIR / f"padron_contribuyentes_tipo_persona_{config.TODAY_STAMP}.csv"
    rows_tipo = [
        ["periodo", "tipo_persona", "total_contribuyentes"],
        ["2023-01", "Física", "78,452,190"],
        ["2023-01", "Moral", "2,351,114"],
        ["2023-04", "Fisica", "79,120,540"],
        ["2023-04", "Moral", "2,380,450"],
        ["2023-07", "F", "79,890,200"],
        ["2023-07", "M", "2,410,030"],
        ["2023-10", "Física", "80,650,110"],
        ["2023-10", "Moral", "2,445,900"],
        ["2024-01", "Física", "81,230,400"],
        ["2024-01", "Moral", "2,480,120"],
    ]
    with open(path_tipo, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(rows_tipo)
    sha_tipo = _sha256(path_tipo)
    res_list.append({
        "file": str(path_tipo),
        "filename": path_tipo.name,
        "source_url": "sample_fallback",
        "size_bytes": path_tipo.stat().st_size,
        "sha256": sha_tipo,
        "resource_name": "padron tipo de persona",
    })

    # Recurso 2: Situación del contribuyente ante el RFC
    path_sit = config.RAW_DIR / f"padron_situacion_contribuyente_{config.TODAY_STAMP}.csv"
    rows_sit = [
        ["periodo", "estatus_rfc", "total_contribuyentes"],
        ["2023-01", "Activo", "45,120,400"],
        ["2023-01", "Suspendido", "33,331,790"],
        ["2023-01", "Cancelado", "2,351,114"],
        ["2023-04", "Activo", "45,890,120"],
        ["2023-04", "Suspendido", "33,230,420"],
        ["2023-04", "Cancelación", "2,380,450"],
        ["2023-07", "A", "46,500,230"],
        ["2023-07", "S", "33,390,000"],
        ["2023-07", "C", "2,409,970"],
        ["2023-10", "Activo", "47,120,450"],
        ["2023-10", "Suspendido", "33,529,660"],
        ["2023-10", "Cancelado", "2,445,900"],
        ["2024-01", "Activo", "47,890,200"],
        ["2024-01", "Suspendido", "33,340,200"],
        ["2024-01", "Cancelado", "2,480,120"],
    ]
    with open(path_sit, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(rows_sit)
    sha_sit = _sha256(path_sit)
    res_list.append({
        "file": str(path_sit),
        "filename": path_sit.name,
        "source_url": "sample_fallback",
        "size_bytes": path_sit.stat().st_size,
        "sha256": sha_sit,
        "resource_name": "padron situacion del contribuyente",
    })

    logger.info("  ✔ Recursos de muestra del Padrón generados exitosamente.")
    return res_list


# ──────────────────────────────────────────────
# 3. Descargar Listado 69-B (con fallback local y muestra)
# ──────────────────────────────────────────────

def _download_69b(logger: logging.Logger) -> dict[str, Any]:
    """Descarga el Listado 69-B, o busca en raw/*69b*.csv, o genera muestra."""
    out_path = config.RAW_DIR / f"listado_69b_{config.TODAY_STAMP}.csv"

    # 1. Intento de red
    try:
        logger.info("  Intentando descarga directa de Listado 69-B...")
        raw_bytes = _http_get(config.URL_69B, logger)
        out_path.write_bytes(raw_bytes)
        sha = _sha256(out_path)
        logger.info("  ✔ Descargado listado 69-B: %s (%d bytes)", out_path.name, out_path.stat().st_size)
        return {
            "file": str(out_path),
            "filename": out_path.name,
            "source": "download",
            "source_url": config.URL_69B,
            "size_bytes": out_path.stat().st_size,
            "sha256": sha,
        }
    except Exception as exc:
        logger.warning("  Descarga directa 69-B no disponible (%s)", exc)

    # 2. Búsqueda local de archivos *69*b*.csv preexistentes
    candidates = sorted(
        [f for f in config.RAW_DIR.glob("*69*b*.csv") if f.stat().st_size > 100 and f != out_path],
        key=lambda p: p.stat().st_mtime, reverse=True
    )
    if candidates:
        local_file = candidates[0]
        sha = _sha256(local_file)
        logger.info("  ✔ Usando archivo local preexistente: %s (%d bytes)", local_file.name, local_file.stat().st_size)
        return {
            "file": str(local_file),
            "filename": local_file.name,
            "source": "local_file",
            "size_bytes": local_file.stat().st_size,
            "sha256": sha,
        }

    # 3. Generación de muestra completa con medios SAT y DOF
    logger.info("  Generando dataset representativo de Listado 69-B (SAT + DOF)...")
    return _generate_sample_69b(out_path, logger)


def _generate_sample_69b(out_path: Path, logger: logging.Logger) -> dict[str, Any]:
    """Genera dataset 69-B con columnas completas (etapas y medios SAT / DOF)."""
    headers = [
        "No", "RFC", "Nombre del Contribuyente", "Situación del contribuyente",
        "Número y fecha de oficio global de presunción",
        "Publicación página SAT presuntos",
        "Publicación DOF presuntos",
        "Número y fecha de oficio global de contribuyentes que desvirtuaron",
        "Publicación página SAT desvirtuados",
        "Publicación DOF desvirtuados",
        "Número y fecha de oficio global de definitivos",
        "Publicación página SAT definitivos",
        "Publicación DOF definitivos",
        "Número y fecha de oficio global de sentencia favorable",
        "Publicación página SAT sentencia favorable",
        "Publicación DOF sentencia favorable",
    ]

    rows = [
        [
            "1", "AAA010101AAA", "EMPRESA EJEMPLO UNO SA DE CV", "Definitivo",
            "500-05-2023-12345 de 15 de marzo de 2023", "01/06/2023", "15/06/2023",
            "", "", "",
            "500-05-2023-67890 de 1 de septiembre de 2023", "15/10/2023", "20/10/2023",
            "", "", ""
        ],
        [
            "2", "BBB020202BBB", "DISTRIBUIDORA EJEMPLO DOS SA", "Presunto",
            "500-05-2024-11111 de 20/01/2024", "15/03/2024", "25/03/2024",
            "", "", "",
            "", "", "",
            "", "", ""
        ],
        [
            "3", "CCC030303CCC", "COMERCIALIZADORA EJEMPLO TRES SC // NOTA: Amparo en revisión 123/2023", "Sentencia Favorable",
            "500-05-2022-22222 de 10 de junio de 2022", "01/08/2022", "10/08/2022",
            "", "", "",
            "500-05-2022-33333 de 15/11/2022", "01/01/2023", "15/01/2023",
            "500-05-2023-44444 de 20 de mayo de 2023", "15/07/2023", "30/07/2023"
        ],
        [
            "4", "DDD040404DDD", "SERVICIOS INTEGRALES EJEMPLO CUATRO SA DE CV", "Desvirtuado",
            "500-05-2024-55555 de 5 de febrero de 2024", "20/04/2024", "30/04/2024",
            "500-05-2024-66666 de 30 de julio de 2024", "15/09/2024", "22/09/2024",
            "", "", "",
            "", "", ""
        ],
        [
            "5", "EEE050505EEE", "CONSTRUCTORA EJEMPLO CINCO SA DE CV", "Definitivo",
            "500-05-2023-77777 de 12/04/2023", "01/07/2023", "10/07/2023",
            "", "", "",
            "500-05-2023-88888 de 25 de octubre de 2023", "10/12/2023", "18/12/2023",
            "", "", ""
        ],
        [
            "6", "AAA010101AAA", "EMPRESA EJEMPLO UNO MODIFICADA SAPI", "Definitivo",
            "500-05-2023-12345 de 15 de marzo de 2023", "01/06/2023", "15/06/2023",
            "", "", "",
            "500-05-2023-67890 de 1 de septiembre de 2023", "15/10/2023", "20/10/2023",
            "", "", ""
        ],
        [
            "7", "RFC_INVALIDO_XYZ", "EMPRESA CON ERROR DE CAPTURA", "Presunto",
            "500-05-2024-00001 de 10/03/2024", "01/05/2024", "05/05/2024",
            "", "", "",
            "", "", "",
            "", "", ""
        ],
        [
            "8", "XAXX010101000", "PUBLICO EN GENERAL FISICA // RFC generico con notas adicionales", "Presunto",
            "500-05-2024-99999 de 18 de enero de 2024", "01/03/2024", "10/03/2024",
            "", "", "",
            "", "", "",
            "", "", ""
        ],
    ]

    with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)

    sha = _sha256(out_path)
    logger.info("  ✔ Archivo 69-B generado: %s (%d filas)", out_path.name, len(rows))
    return {
        "file": str(out_path),
        "filename": out_path.name,
        "source": "sample_generated",
        "size_bytes": out_path.stat().st_size,
        "sha256": sha,
    }


# ──────────────────────────────────────────────
# 4. Manifest
# ──────────────────────────────────────────────

def _save_manifest(entries: list[dict], logger: logging.Logger) -> Path:
    """Guarda manifest.json con trazabilidad de hashes y tamaños."""
    manifest_path = config.RAW_DIR / "manifest.json"
    manifest = {
        "pipeline_run_ts": config.PIPELINE_RUN_TS,
        "generated_at": config.PIPELINE_RUN_TS,
        "files_count": len(entries),
        "files": entries,
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("  ✔ Manifest generado: %s (%d archivos registrados)", manifest_path.name, len(entries))
    return manifest_path


# ──────────────────────────────────────────────
# 5. Carga espejo a SQLite (all-TEXT, intacto)
# ──────────────────────────────────────────────

def _load_csv_to_raw_table(
    db_path: Path, csv_path: Path, table_name: str, logger: logging.Logger
) -> int:
    """
    Carga un CSV completo e intacto en SQLite (todas las columnas TEXT).
    Esquema espejo e idempotente: DROP + CREATE.
    """
    if not csv_path.exists():
        logger.warning("  Archivo inexistente para tabla [%s]: %s", table_name, csv_path.name)
        return 0

    enc = detect_encoding(csv_path)
    with open(csv_path, "r", encoding=enc, errors="replace") as f:
        reader = csv.reader(f)
        try:
            headers = next(reader)
        except StopIteration:
            return 0
        all_rows = list(reader)

    safe_headers = []
    seen = set()
    for idx, h in enumerate(headers):
        c = h.strip().lower()
        c = "".join(char if char.isalnum() or char == "_" else "_" for char in c).strip("_")
        if not c:
            c = f"col_{idx}"
        if c in seen:
            c = f"{c}_{idx}"
        seen.add(c)
        safe_headers.append(c)

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON;")
    try:
        conn.execute(f"DROP TABLE IF EXISTS [{table_name}];")
        cols_def = ", ".join(f"[{h}] TEXT" for h in safe_headers)
        conn.execute(f"CREATE TABLE [{table_name}] ({cols_def});")

        placeholders = ", ".join("?" * len(safe_headers))
        insert_sql = f"INSERT INTO [{table_name}] VALUES ({placeholders});"

        loaded = 0
        for row in all_rows:
            padded = row[:len(safe_headers)]
            while len(padded) < len(safe_headers):
                padded.append("")
            conn.execute(insert_sql, padded)
            loaded += 1

        conn.commit()
        logger.info("  ✔ Tabla espejo [%s] en %s: %d filas cargadas", table_name, db_path.name, loaded)
        return loaded
    finally:
        conn.close()


# ──────────────────────────────────────────────
# Punto de entrada principal de la fase EXTRACT
# ──────────────────────────────────────────────

def run_extract(logger: logging.Logger) -> dict[str, Any]:
    """Punto de entrada de extracción."""
    logger.info("=" * 60)
    logger.info("FASE 1: EXTRACCIÓN (raw/ y tablas espejo en dw_sat.db)")
    logger.info("=" * 60)

    manifest_entries: list[dict] = []

    # 1. Metadatos CKAN
    logger.info("[Paso 1/3] Consultando metadatos CKAN del Padrón de Contribuyentes")
    ckan_data = _fetch_ckan_metadata(logger)

    # 2. Descargar recursos del Padrón
    logger.info("[Paso 2/3] Descargando recursos de alta prioridad del Padrón")
    padron_entries = []
    if ckan_data:
        priority_res = _filter_priority_resources(ckan_data, logger)
        for idx, res in enumerate(priority_res, 1):
            downloaded = _download_padron_resource(res, idx, logger)
            if downloaded:
                padron_entries.append(downloaded)
                manifest_entries.append(downloaded)

    if not padron_entries:
        # Fallback si no hay red o CKAN no devolvió recursos
        sample_padron = _generate_sample_padron(logger)
        padron_entries.extend(sample_padron)
        manifest_entries.extend(sample_padron)

    # 3. Descargar listado 69-B
    logger.info("[Paso 3/3] Obteniendo Listado Art. 69-B")
    entry_69b = _download_69b(logger)
    manifest_entries.append(entry_69b)

    # 4. Manifest
    _save_manifest(manifest_entries, logger)

    # 5. Carga a tablas espejo en dw/dw_sat.db
    logger.info("Cargando tablas espejo intactas (TEXT) en %s...", config.DW_DB_PATH.name)
    rows_69b = _load_csv_to_raw_table(config.DW_DB_PATH, Path(entry_69b["file"]), "raw_69b", logger)

    rows_padron_tipo = 0
    rows_padron_sit = 0
    for pe in padron_entries:
        path = Path(pe["file"])
        name = pe.get("resource_name", "").lower()
        if "tipo" in name or "persona" in name:
            rows_padron_tipo += _load_csv_to_raw_table(config.DW_DB_PATH, path, "raw_padron_tipo", logger)
        elif "situacion" in name or "situación" in name or "estatus" in name:
            rows_padron_sit += _load_csv_to_raw_table(config.DW_DB_PATH, path, "raw_padron_situacion", logger)
        else:
            tbl_name = "raw_padron_" + path.stem[:25].replace("-", "_")
            _load_csv_to_raw_table(config.DW_DB_PATH, path, tbl_name, logger)

    return {
        "manifest": str(config.RAW_DIR / "manifest.json"),
        "raw_69b_rows": rows_69b,
        "raw_padron_tipo_rows": rows_padron_tipo,
        "raw_padron_situacion_rows": rows_padron_sit,
        "status": "OK",
    }
