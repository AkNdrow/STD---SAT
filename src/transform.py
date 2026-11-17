"""
src/transform.py — Fase TRANSFORM del pipeline ETL SAT 69-B
============================================================
Transforma, limpia, normaliza y despivotea los datos crudos de raw/.

Responsabilidades:
  1. Rama 69-B:
     - Limpieza y validación de RFC (regex estricta).
     - Derivación de tipo_persona (Moral len=12, Física len=13).
     - Limpieza de nombres y separación de notas posteriores a '//' hacia nota_sat.
     - Parseo de fechas en español (e.g. '15 de marzo de 2023', '01/06/2023') a ISO (AAAA-MM-DD).
     - Despivoteo dinámico ancho a largo (4 etapas: Presunto, Desvirtuado, Definitivo,
       Sentencia Favorable) x 2 medios (SAT y DOF).
     - Trazabilidad total: registros rechazados van a clean/rechazados_69b.csv con motivo exacto.
     - Detección de duplicados y registro de conflictos de nombres para un mismo RFC
       en clean/conflictos_rfc.csv.
     - Salidas: clean/contribuyentes_limpios.csv y clean/publicaciones_long.csv.
  2. Rama Padrón:
     - Normalización de periodos a AAAA-MM-01 derivando anio y trimestre.
     - Homologación de tipo de persona (Física, Moral) y estatus ante el RFC (Activo, Suspendido, Cancelado).
     - Conversión de cifras numéricas con comas a enteros limpios.
     - Salida: clean/padron_universo_limpio.csv.
  3. Resumen y Balance:
     - Impresión y guardado de métricas en clean/resumen_transformacion.json.

Solo biblioteca estándar: csv, json, re, pathlib, logging, datetime.
"""

import csv
import json
import logging
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

import config
from src.extract import detect_encoding


# ──────────────────────────────────────────────
# Expresiones Regulares y Validaciones
# ──────────────────────────────────────────────

RFC_RE = re.compile(config.RFC_REGEX)

# Patrón para separar número y fecha de oficio:
# ej: "500-05-2023-12345 de 15 de marzo de 2023" o "500-05-2024-11111 de 20/01/2024"
OFICIO_SPLIT_RE = re.compile(
    r"^([A-Z0-9\-\./_]+)\s+de\s+(.+)$",
    re.IGNORECASE
)

# Patrones de fechas
FECHA_TEXTO_ES_RE = re.compile(
    r"(\d{1,2})\s+de\s+([a-záéíóúñ]+)\s+de\s+(\d{4})",
    re.IGNORECASE
)
FECHA_SLASH_RE = re.compile(r"(\d{1,2})/(\d{1,2})/(\d{4})")
FECHA_DASH_RE = re.compile(r"(\d{4})-(\d{1,2})-(\d{1,2})")
FECHA_DASH_INV_RE = re.compile(r"(\d{1,2})-(\d{1,2})-(\d{4})")


# ──────────────────────────────────────────────
# Funciones Utilitarias de Limpieza y Fechas
# ──────────────────────────────────────────────

def parse_fecha_iso(fecha_raw: str) -> str:
    """
    Convierte cualquier formato de fecha común o en texto en español a ISO 'AAAA-MM-DD'.
    Retorna '' si la cadena está vacía o no se puede interpretar.
    """
    s = (fecha_raw or "").strip()
    if not s:
        return ""

    # Caso 1: '15 de marzo de 2023'
    m_txt = FECHA_TEXTO_ES_RE.search(s)
    if m_txt:
        dia = int(m_txt.group(1))
        mes_nombre = m_txt.group(2).lower()
        mes = config.MESES_ES.get(mes_nombre, 0)
        anio = int(m_txt.group(3))
        if mes > 0 and 1 <= dia <= 31:
            return f"{anio:04d}-{mes:02d}-{dia:02d}"

    # Caso 2: '15/03/2023' o '1/6/2023' (DD/MM/YYYY)
    m_slash = FECHA_SLASH_RE.search(s)
    if m_slash:
        dia, mes, anio = int(m_slash.group(1)), int(m_slash.group(2)), int(m_slash.group(3))
        if 1 <= mes <= 12 and 1 <= dia <= 31:
            return f"{anio:04d}-{mes:02d}-{dia:02d}"

    # Caso 3: '2023-03-15' (YYYY-MM-DD)
    m_dash = FECHA_DASH_RE.search(s)
    if m_dash:
        anio, mes, dia = int(m_dash.group(1)), int(m_dash.group(2)), int(m_dash.group(3))
        if 1 <= mes <= 12 and 1 <= dia <= 31:
            return f"{anio:04d}-{mes:02d}-{dia:02d}"

    # Caso 4: '15-03-2023' (DD-MM-YYYY)
    m_dash_inv = FECHA_DASH_INV_RE.search(s)
    if m_dash_inv:
        dia, mes, anio = int(m_dash_inv.group(1)), int(m_dash_inv.group(2)), int(m_dash_inv.group(3))
        if 1 <= mes <= 12 and 1 <= dia <= 31:
            return f"{anio:04d}-{mes:02d}-{dia:02d}"

    return ""


def parse_oficio_campo(oficio_raw: str) -> tuple[str, str]:
    """
    Descompone un campo como '500-05-2023-12345 de 15 de marzo de 2023'
    en (numero_oficio, fecha_oficio_iso).
    """
    s = (oficio_raw or "").strip()
    if not s:
        return ("", "")

    m = OFICIO_SPLIT_RE.match(s)
    if m:
        num_oficio = m.group(1).strip()
        fecha_txt = m.group(2).strip()
        fecha_iso = parse_fecha_iso(fecha_txt)
        return (num_oficio, fecha_iso)

    # Si no tiene patrón 'de', retornar todo como número de oficio
    return (s, "")


def limpiar_nombre_y_nota(nombre_raw: str) -> tuple[str, str]:
    """
    Limpia el nombre del contribuyente y separa el contenido posterior
    a '//' hacia el campo nota_sat.
    """
    s = (nombre_raw or "").strip()
    nota = ""
    if "//" in s:
        partes = s.split("//", 1)
        nombre = partes[0].strip()
        nota = partes[1].strip()
    else:
        nombre = s

    # Normalizar espacios
    nombre_limpio = re.sub(r"\s+", " ", nombre).upper()
    nota_limpia = re.sub(r"\s+", " ", nota)
    return nombre_limpio, nota_limpia


def derivar_tipo_persona(rfc: str) -> str:
    """Moral si len=12, Física si len=13."""
    if len(rfc) == 12:
        return "Moral"
    elif len(rfc) == 13:
        return "Física"
    return "Moral"


# ──────────────────────────────────────────────
# Mapeo Dinámico de Columnas para Despivoteo
# ──────────────────────────────────────────────

ETAPAS = ["Presunto", "Desvirtuado", "Definitivo", "Sentencia Favorable"]
MEDIOS = ["SAT", "DOF"]


def _normalizar_texto(t: str) -> str:
    """Minúsculas, quita acentos y puntuación."""
    t = t.lower()
    t = t.replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u")
    return t


def detectar_columnas_etapas(headers: list[str]) -> dict[str, dict[str, Any]]:
    """
    Identifica dinámicamente qué columna contiene:
      - oficio global para cada etapa
      - fecha publicación SAT para cada etapa
      - fecha publicación DOF para cada etapa
    Retorna diccionario indexado por etapa.
    """
    mapping: dict[str, dict[str, Any]] = {
        etapa: {
            "col_oficio": None,
            "col_pub_sat": None,
            "col_pub_dof": None,
        }
        for etapa in ETAPAS
    }

    # Palabras clave por etapa
    etapa_keywords = {
        "Presunto": ["presun"],
        "Desvirtuado": ["desvirtua"],
        "Definitivo": ["definiti"],
        "Sentencia Favorable": ["sentencia", "favorable"],
    }

    for idx, col in enumerate(headers):
        norm = _normalizar_texto(col)

        # Determinar a qué etapa corresponde la columna
        etapa_match = None
        for etapa, kws in etapa_keywords.items():
            if any(kw in norm for kw in kws):
                etapa_match = etapa
                break

        if not etapa_match:
            continue

        # Clasificar según el contenido
        if "oficio" in norm or "global" in norm:
            mapping[etapa_match]["col_oficio"] = idx
        elif "dof" in norm or "diario oficial" in norm:
            mapping[etapa_match]["col_pub_dof"] = idx
        elif "sat" in norm or "pagina" in norm or "publicacion" in norm:
            mapping[etapa_match]["col_pub_sat"] = idx

    return mapping


# ──────────────────────────────────────────────
# Transformación de Rama 69-B
# ──────────────────────────────────────────────

def transformar_rama_69b(
    raw_file: Path, logger: logging.Logger
) -> tuple[list[dict], list[dict], list[dict], list[dict], dict[str, int]]:
    """
    Procesa el archivo crudo del 69-B:
      - Valida RFC (regex estricta).
      - Rechaza con motivo exacto.
      - Limpia nombre y extrae notas SAT.
      - Despivotea hacia publicaciones_long.
      - Detecta conflictos de RFC con múltiples nombres.
    """
    enc = detect_encoding(raw_file)
    logger.info("  Procesando 69-B desde: %s (codificación: %s)", raw_file.name, enc)

    with open(raw_file, "r", encoding=enc, errors="replace") as f:
        reader = csv.reader(f)
        try:
            headers = next(reader)
        except StopIteration:
            return [], [], [], [], {}
        rows = list(reader)

    # Identificar columnas clave (RFC, Nombre, Situación)
    col_rfc = 1
    col_nombre = 2
    col_situacion = 3
    for idx, h in enumerate(headers):
        hn = _normalizar_texto(h)
        if hn in ("rfc",):
            col_rfc = idx
        elif "nombre" in hn or "razon social" in hn:
            col_nombre = idx
        elif "situacion" in hn or "estatus" in hn:
            col_situacion = idx

    # Mapeo de etapas para despivoteo
    col_mapping = detectar_columnas_etapas(headers)
    logger.info("  Mapeo de columnas detectado para despivoteo:")
    for etapa, m in col_mapping.items():
        logger.info(
            "    %-20s → Oficio: col %s | Pub SAT: col %s | Pub DOF: col %s",
            etapa, m["col_oficio"], m["col_pub_sat"], m["col_pub_dof"]
        )

    contribuyentes_map: dict[str, dict[str, Any]] = {}
    nombres_por_rfc: dict[str, set[str]] = defaultdict(set)
    publicaciones_long: list[dict] = []
    rechazados: list[dict] = []
    conflictos_rfc: list[dict] = []

    conteo_rechazos = defaultdict(int)
    filas_leidas = len(rows)

    seen_long_tuples = set()

    for row_idx, row in enumerate(rows, start=2):
        if not row or not any(row):
            rechazados.append({
                "fila_origen": row_idx,
                "rfc": "",
                "nombre_crudo": "",
                "motivo_rechazo": "Fila completamente vacía",
                "datos_originales": json.dumps(row, ensure_ascii=False),
            })
            conteo_rechazos["Fila completamente vacía"] += 1
            continue

        raw_rfc = row[col_rfc].strip().upper() if col_rfc < len(row) else ""
        raw_nombre = row[col_nombre].strip() if col_nombre < len(row) else ""
        raw_situacion = row[col_situacion].strip() if col_situacion < len(row) else ""

        # 1. Validación de RFC
        if not raw_rfc:
            rechazados.append({
                "fila_origen": row_idx,
                "rfc": "",
                "nombre_crudo": raw_nombre,
                "motivo_rechazo": "RFC ausente o vacío",
                "datos_originales": json.dumps(row, ensure_ascii=False),
            })
            conteo_rechazos["RFC ausente o vacío"] += 1
            continue

        if not RFC_RE.match(raw_rfc):
            rechazados.append({
                "fila_origen": row_idx,
                "rfc": raw_rfc,
                "nombre_crudo": raw_nombre,
                "motivo_rechazo": "RFC inválido",
                "datos_originales": json.dumps(row, ensure_ascii=False),
            })
            conteo_rechazos["RFC inválido"] += 1
            continue

        # 2. Limpieza de Nombre y Nota SAT
        nombre_limpio, nota_sat = limpiar_nombre_y_nota(raw_nombre)
        tipo_persona = derivar_tipo_persona(raw_rfc)

        # 3. Registro de Contribuyente y detección de conflictos
        nombres_por_rfc[raw_rfc].add(nombre_limpio)
        if raw_rfc not in contribuyentes_map:
            contribuyentes_map[raw_rfc] = {
                "rfc": raw_rfc,
                "nombre": nombre_limpio,
                "tipo_persona": tipo_persona,
                "nota_sat": nota_sat,
                "situacion_actual": raw_situacion or "Presunto",
            }
        else:
            # Si ya existía pero con otro nombre, se registra conflicto
            prev_nombre = contribuyentes_map[raw_rfc]["nombre"]
            if prev_nombre != nombre_limpio:
                conflictos_rfc.append({
                    "rfc": raw_rfc,
                    "nombre_registrado": prev_nombre,
                    "nombre_conflicto": nombre_limpio,
                    "fila_origen": row_idx,
                    "motivo": "Mismo RFC reportado con diferente razón social",
                })

        # 4. Despivoteo (Ancho a Largo)
        for etapa in ETAPAS:
            m = col_mapping[etapa]
            col_of = m["col_oficio"]
            col_sat = m["col_pub_sat"]
            col_dof = m["col_pub_dof"]

            oficio_raw = row[col_of].strip() if col_of is not None and col_of < len(row) else ""
            num_oficio, fecha_oficio = parse_oficio_campo(oficio_raw)

            fecha_sat_raw = row[col_sat].strip() if col_sat is not None and col_sat < len(row) else ""
            fecha_sat_iso = parse_fecha_iso(fecha_sat_raw)

            fecha_dof_raw = row[col_dof].strip() if col_dof is not None and col_dof < len(row) else ""
            fecha_dof_iso = parse_fecha_iso(fecha_dof_raw)

            # Publicación SAT
            if fecha_sat_iso or (num_oficio and (col_sat is not None or not fecha_dof_iso)):
                tup_sat = (raw_rfc, etapa, "SAT", num_oficio, fecha_oficio, fecha_sat_iso)
                if tup_sat not in seen_long_tuples and (num_oficio or fecha_sat_iso):
                    seen_long_tuples.add(tup_sat)
                    publicaciones_long.append({
                        "rfc": raw_rfc,
                        "situacion_69b": etapa,
                        "medio": "SAT",
                        "numero_oficio": num_oficio,
                        "fecha_oficio": fecha_oficio,
                        "fecha_publicacion": fecha_sat_iso,
                    })

            # Publicación DOF
            if fecha_dof_iso:
                tup_dof = (raw_rfc, etapa, "DOF", num_oficio, fecha_oficio, fecha_dof_iso)
                if tup_dof not in seen_long_tuples:
                    seen_long_tuples.add(tup_dof)
                    publicaciones_long.append({
                        "rfc": raw_rfc,
                        "situacion_69b": etapa,
                        "medio": "DOF",
                        "numero_oficio": num_oficio,
                        "fecha_oficio": fecha_oficio,
                        "fecha_publicacion": fecha_dof_iso,
                    })

    contribuyentes_limpios = list(contribuyentes_map.values())
    stats = {
        "filas_leidas": filas_leidas,
        "contribuyentes_unicos": len(contribuyentes_limpios),
        "publicaciones_long": len(publicaciones_long),
        "rechazados": len(rechazados),
        "conflictos_rfc": len(conflictos_rfc),
    }

    return (
        contribuyentes_limpios,
        publicaciones_long,
        rechazados,
        conflictos_rfc,
        stats,
    )


# ──────────────────────────────────────────────
# Transformación de Rama Padrón
# ──────────────────────────────────────────────

def normalizar_periodo(periodo_raw: str) -> tuple[str, str, int, int, int]:
    """
    Convierte periodos como '2023-01', '2023/04', '202307'
    a (periodo_normalizado, fecha_corte_iso, anio, trimestre, mes).
    Formato fecha corte: 'AAAA-MM-01'.
    """
    s = (periodo_raw or "").strip()
    digits = re.findall(r"\d+", s)
    if not digits:
        return (s, "", 0, 0, 0)

    if len(digits) >= 2:
        anio = int(digits[0])
        mes = int(digits[1])
    elif len(digits) == 1 and len(digits[0]) == 6:
        # Formato YYYYMM
        anio = int(digits[0][:4])
        mes = int(digits[0][4:])
    else:
        anio = int(digits[0])
        mes = 1

    mes = max(1, min(12, mes))
    trimestre = (mes - 1) // 3 + 1
    fecha_corte = f"{anio:04d}-{mes:02d}-01"
    periodo_norm = f"{anio:04d}-{mes:02d}"
    return (periodo_norm, fecha_corte, anio, trimestre, mes)


def limpiar_entero(cifra_raw: str) -> int:
    """Convierte cadenas con comas o puntos (e.g. '78,452,190') a int."""
    s = (cifra_raw or "").strip().replace(",", "").replace(" ", "")
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return 0


def homologar_tipo_persona(tipo_raw: str) -> str:
    """Homologa a 'Física' o 'Moral'."""
    s = (tipo_raw or "").strip().lower()
    return config.HOMOLOGA_TIPO_PERSONA.get(s, tipo_raw.strip().title() or "Indeterminado")


def homologar_estatus_rfc(estatus_raw: str) -> str:
    """Homologa a 'Activo', 'Suspendido' o 'Cancelado'."""
    s = (estatus_raw or "").strip().lower()
    return config.HOMOLOGA_ESTATUS_RFC.get(s, estatus_raw.strip().title() or "Indeterminado")


def transformar_rama_padron(logger: logging.Logger) -> list[dict]:
    """
    Procesa todos los CSVs relacionados al Padrón en raw/ y genera
    registros normalizados para clean/padron_universo_limpio.csv.
    """
    padron_files = sorted(config.RAW_DIR.glob("*padron*.csv"))
    if not padron_files:
        logger.warning("  No se encontraron archivos de Padrón en raw/.")
        return []

    registros_limpios: list[dict] = []

    for pf in padron_files:
        enc = detect_encoding(pf)
        logger.info("  Procesando Padrón: %s (codificación: %s)", pf.name, enc)

        with open(pf, "r", encoding=enc, errors="replace") as f:
            reader = csv.reader(f)
            try:
                headers = next(reader)
            except StopIteration:
                continue
            rows = list(reader)

        # Detectar columnas
        col_periodo = 0
        col_tipo = None
        col_estatus = None
        col_total = None

        for idx, h in enumerate(headers):
            hn = _normalizar_texto(h)
            if "periodo" in hn or "fecha" in hn or "año" in hn or "anio" in hn:
                col_periodo = idx
            elif "tipo" in hn or "persona" in hn:
                col_tipo = idx
            elif "situacion" in hn or "estatus" in hn:
                col_estatus = idx
            elif "total" in hn or "contribuyente" in hn or "cantidad" in hn or "cifra" in hn:
                col_total = idx

        if col_total is None:
            col_total = len(headers) - 1

        for row in rows:
            if not row or not any(row):
                continue

            raw_periodo = row[col_periodo].strip() if col_periodo < len(row) else ""
            periodo_norm, fecha_corte, anio, trimestre, mes = normalizar_periodo(raw_periodo)

            tipo_persona = "Indeterminado"
            if col_tipo is not None and col_tipo < len(row):
                tipo_persona = homologar_tipo_persona(row[col_tipo])

            estatus_rfc = "Indeterminado"
            if col_estatus is not None and col_estatus < len(row):
                estatus_rfc = homologar_estatus_rfc(row[col_estatus])

            total_val = row[col_total].strip() if col_total < len(row) else "0"
            total_clean = limpiar_entero(total_val)

            registros_limpios.append({
                "fuente_recurso": pf.name,
                "periodo_original": raw_periodo,
                "fecha_corte": fecha_corte,
                "anio": str(anio),
                "trimestre": str(trimestre),
                "mes": str(mes),
                "tipo_persona": tipo_persona,
                "estatus_rfc": estatus_rfc,
                "total_contribuyentes": str(total_clean),
            })

    return registros_limpios


# ──────────────────────────────────────────────
# Escritura de Archivos Limpios (CSV y JSON)
# ──────────────────────────────────────────────

def _guardar_csv(filepath: Path, fieldnames: list[str], rows: list[dict], logger: logging.Logger) -> None:
    """Escribe un archivo CSV con codificación UTF-8 e indicador de trazabilidad."""
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    logger.info("  ✔ Archivo generado: %-32s (%d filas)", filepath.name, len(rows))


# ──────────────────────────────────────────────
# Punto de Entrada Principal: run_transform
# ──────────────────────────────────────────────

def run_transform(logger: logging.Logger) -> dict[str, Any]:
    """Ejecuta la fase de transformación completa."""
    logger.info("=" * 60)
    logger.info("FASE 3: TRANSFORMACIÓN (Normalización, Despivoteo y Trazabilidad)")
    logger.info("=" * 60)

    # ── 1. Rama 69-B ──
    logger.info("[Paso 1/2] Procesando Rama Art. 69-B del CFF...")
    candidates_69b = sorted(
        [f for f in config.RAW_DIR.glob("*69*b*.csv") if f.stat().st_size > 100],
        key=lambda p: p.stat().st_mtime, reverse=True
    )
    if not candidates_69b:
        raise FileNotFoundError("No se encontró ningún archivo de entrada 69-B en raw/")

    file_69b = candidates_69b[0]
    (
        contribuyentes,
        publicaciones_long,
        rechazados,
        conflictos_rfc,
        stats_69b,
    ) = transformar_rama_69b(file_69b, logger)

    # Guardar clean/contribuyentes_limpios.csv
    file_contribuyentes = config.CLEAN_DIR / "contribuyentes_limpios.csv"
    _guardar_csv(
        file_contribuyentes,
        ["rfc", "nombre", "tipo_persona", "nota_sat", "situacion_actual"],
        contribuyentes,
        logger,
    )

    # Guardar clean/publicaciones_long.csv
    file_pub_long = config.CLEAN_DIR / "publicaciones_long.csv"
    _guardar_csv(
        file_pub_long,
        ["rfc", "situacion_69b", "medio", "numero_oficio", "fecha_oficio", "fecha_publicacion"],
        publicaciones_long,
        logger,
    )

    # Guardar clean/rechazados_69b.csv (Trazabilidad total)
    file_rechazados = config.CLEAN_DIR / "rechazados_69b.csv"
    _guardar_csv(
        file_rechazados,
        ["fila_origen", "rfc", "nombre_crudo", "motivo_rechazo", "datos_originales"],
        rechazados,
        logger,
    )

    # Guardar clean/conflictos_rfc.csv
    file_conflictos = config.CLEAN_DIR / "conflictos_rfc.csv"
    _guardar_csv(
        file_conflictos,
        ["rfc", "nombre_registrado", "nombre_conflicto", "fila_origen", "motivo"],
        conflictos_rfc,
        logger,
    )

    # ── 2. Rama Padrón ──
    logger.info("[Paso 2/2] Procesando Rama Padrón de Contribuyentes...")
    padron_limpio = transformar_rama_padron(logger)
    file_padron = config.CLEAN_DIR / "padron_universo_limpio.csv"
    _guardar_csv(
        file_padron,
        [
            "fuente_recurso", "periodo_original", "fecha_corte", "anio",
            "trimestre", "mes", "tipo_persona", "estatus_rfc", "total_contribuyentes"
        ],
        padron_limpio,
        logger,
    )

    # ── 3. Resumen y Balance de Transformación ──
    resumen = {
        "pipeline_run_ts": config.PIPELINE_RUN_TS,
        "rama_69b": {
            "archivo_origen": file_69b.name,
            "filas_leidas": stats_69b["filas_leidas"],
            "filas_validas": len(contribuyentes),
            "filas_rechazadas": len(rechazados),
            "motivos_rechazo": {
                r["motivo_rechazo"]: sum(1 for x in rechazados if x["motivo_rechazo"] == r["motivo_rechazo"])
                for r in rechazados
            },
            "conflictos_rfc_distintos_nombres": len(conflictos_rfc),
            "filas_despivoteadas_formato_largo": len(publicaciones_long),
        },
        "rama_padron": {
            "archivos_procesados": len(list(config.RAW_DIR.glob("*padron*.csv"))),
            "filas_generadas": len(padron_limpio),
        },
        "archivos_generados": [
            str(file_contribuyentes.name),
            str(file_pub_long.name),
            str(file_rechazados.name),
            str(file_conflictos.name),
            str(file_padron.name),
        ],
    }

    resumen_path = config.CLEAN_DIR / "resumen_transformacion.json"
    resumen_path.write_text(json.dumps(resumen, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("  ✔ Resumen de balance guardado: %s", resumen_path.name)

    # Impresión en consola del balance
    logger.info("=" * 60)
    logger.info("BALANCE DE TRANSFORMACIÓN:")
    logger.info("  [69-B] Filas leídas:            %d", stats_69b["filas_leidas"])
    logger.info("  [69-B] Contribuyentes limpios:   %d", len(contribuyentes))
    logger.info("  [69-B] Publicaciones (long):     %d", len(publicaciones_long))
    logger.info("  [69-B] Registros rechazados:     %d", len(rechazados))
    logger.info("  [69-B] Conflictos RFC detectados: %d", len(conflictos_rfc))
    logger.info("  [PADRÓN] Universo registros:     %d", len(padron_limpio))
    logger.info("=" * 60)

    return resumen
