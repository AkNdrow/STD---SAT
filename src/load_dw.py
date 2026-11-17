"""
src/load_dw.py — Fase LOAD DW del pipeline
============================================
Crea el esquema estrella en SQLite y carga los datos limpios
desde clean/ al Data Warehouse en dw/dw_sat.db.

Esquema estrella:
  ┌──────────────┐     ┌──────────────────────┐     ┌──────────────┐
  │ dim_situacion │◄────│  fact_listado_69b     │────►│ dim_tiempo   │
  └──────────────┘     └──────────────────────┘     └──────────────┘
                                │
                                ▼
                       ┌──────────────────┐
                       │ dim_contribuyente │
                       └──────────────────┘

Recarga idempotente: DROP + CREATE de las tablas dimensionales y de hechos,
preservando intactas las tablas espejo 'raw_*' generadas en la fase de extracción.
"""

import csv
import hashlib
import logging
import sqlite3
from datetime import datetime as dt
from pathlib import Path
from typing import Any

import config


# ──────────────────────────────────────────────
# DDL — Definición del esquema estrella
# ──────────────────────────────────────────────

DDL_STATEMENTS = [
    # ── Dimensión: Situación del contribuyente ──
    """
    CREATE TABLE IF NOT EXISTS dim_situacion (
        situacion_id     INTEGER PRIMARY KEY,
        situacion_nombre TEXT NOT NULL UNIQUE,
        descripcion      TEXT,
        nivel_riesgo     TEXT CHECK(nivel_riesgo IN ('ALTO','MEDIO','BAJO','NULO'))
    );
    """,

    # ── Dimensión: Tiempo (fecha) ──
    """
    CREATE TABLE IF NOT EXISTS dim_tiempo (
        fecha_id         TEXT PRIMARY KEY,  -- formato YYYY-MM-DD
        anio             INTEGER NOT NULL,
        mes              INTEGER NOT NULL,
        dia              INTEGER NOT NULL,
        trimestre        INTEGER NOT NULL,
        nombre_mes       TEXT NOT NULL,
        dia_semana       TEXT NOT NULL
    );
    """,

    # ── Dimensión: Contribuyente ──
    """
    CREATE TABLE IF NOT EXISTS dim_contribuyente (
        contribuyente_id INTEGER PRIMARY KEY AUTOINCREMENT,
        rfc              TEXT NOT NULL UNIQUE,
        nombre           TEXT NOT NULL,
        tipo_persona     TEXT CHECK(tipo_persona IN ('Física','Moral','FISICA','MORAL')),
        nota_sat         TEXT,
        longitud_rfc     INTEGER
    );
    """,

    # ── Tabla de hechos: Listado 69-B (Atómica por evento/publicación) ──
    """
    CREATE TABLE IF NOT EXISTS fact_listado_69b (
        fact_id              INTEGER PRIMARY KEY AUTOINCREMENT,
        row_hash             TEXT NOT NULL UNIQUE,
        contribuyente_id     INTEGER NOT NULL,
        situacion_id         INTEGER NOT NULL,
        medio                TEXT NOT NULL,
        numero_oficio        TEXT,
        fecha_oficio_id      TEXT,
        fecha_publicacion_id TEXT,
        fecha_presuncion_id  TEXT,
        fecha_definitivo_id  TEXT,
        fecha_sentencia_id   TEXT,
        fecha_desvirtuado_id TEXT,
        num_oficio_presuncion TEXT,
        num_oficio_definitivos TEXT,
        pipeline_ts          TEXT NOT NULL,
        FOREIGN KEY (contribuyente_id)     REFERENCES dim_contribuyente(contribuyente_id),
        FOREIGN KEY (situacion_id)         REFERENCES dim_situacion(situacion_id),
        FOREIGN KEY (fecha_publicacion_id) REFERENCES dim_tiempo(fecha_id),
        FOREIGN KEY (fecha_oficio_id)      REFERENCES dim_tiempo(fecha_id)
    );
    """,

    # ── Tabla de metadatos de corrida ──
    """
    CREATE TABLE IF NOT EXISTS meta_pipeline_runs (
        run_id         INTEGER PRIMARY KEY AUTOINCREMENT,
        run_ts         TEXT NOT NULL,
        phase          TEXT NOT NULL,
        records_loaded INTEGER,
        status         TEXT
    );
    """,
]

INDEX_STATEMENTS = [
    "CREATE INDEX IF NOT EXISTS idx_fact_contribuyente ON fact_listado_69b(contribuyente_id);",
    "CREATE INDEX IF NOT EXISTS idx_fact_situacion ON fact_listado_69b(situacion_id);",
    "CREATE INDEX IF NOT EXISTS idx_fact_fecha_pub ON fact_listado_69b(fecha_publicacion_id);",
    "CREATE INDEX IF NOT EXISTS idx_contrib_rfc ON dim_contribuyente(rfc);",
]

MESES = [
    "", "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
    "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre",
]
DIAS_SEMANA = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]


# ──────────────────────────────────────────────
# Inicialización y DDL
# ──────────────────────────────────────────────

def _init_database(db_path: Path, logger: logging.Logger) -> sqlite3.Connection:
    """Inicializa las tablas del esquema estrella sin borrar las tablas espejo raw_*."""
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")

    # Recreación idempotente de tablas dimensionales
    conn.execute("DROP TABLE IF EXISTS fact_listado_69b;")
    conn.execute("DROP TABLE IF EXISTS dim_contribuyente;")
    conn.execute("DROP TABLE IF EXISTS dim_tiempo;")
    conn.execute("DROP TABLE IF EXISTS dim_situacion;")

    for ddl in DDL_STATEMENTS:
        conn.execute(ddl)

    for idx in INDEX_STATEMENTS:
        conn.execute(idx)

    conn.commit()
    logger.info("  ✔ Esquema estrella recreado en %s (tablas raw_* preservadas)", db_path.name)
    return conn


def _load_dim_situacion(conn: sqlite3.Connection, logger: logging.Logger) -> None:
    """Carga catálogo de situaciones."""
    riesgo_map = {
        "Presunto": "ALTO",
        "Definitivo": "ALTO",
        "Sentencia Favorable": "BAJO",
        "Desvirtuado": "NULO",
    }
    desc_map = {
        "Presunto": "Contribuyente con presunción de operaciones inexistentes",
        "Definitivo": "Contribuyente con determinación definitiva de EFOS",
        "Sentencia Favorable": "Contribuyente con sentencia favorable del tribunal",
        "Desvirtuado": "Contribuyente que desvirtuó la presunción ante el SAT",
    }

    for nombre, sid in config.SITUACIONES_CATALOGO.items():
        conn.execute(
            """INSERT OR REPLACE INTO dim_situacion
               (situacion_id, situacion_nombre, descripcion, nivel_riesgo)
               VALUES (?, ?, ?, ?)""",
            (sid, nombre, desc_map.get(nombre, ""), riesgo_map.get(nombre, "MEDIO")),
        )
    conn.commit()
    logger.info("  dim_situacion: %d registros cargados", len(config.SITUACIONES_CATALOGO))


def _load_dim_tiempo(conn: sqlite3.Connection, fechas: set[str], logger: logging.Logger) -> None:
    """Carga dimensión temporal con fechas ISO."""
    loaded = 0
    for fecha_str in sorted(fechas):
        if not fecha_str or len(fecha_str) != 10:
            continue
        try:
            d = dt.strptime(fecha_str, "%Y-%m-%d")
            trimestre = (d.month - 1) // 3 + 1
            dia_semana = DIAS_SEMANA[d.weekday()]
            nombre_mes = MESES[d.month]

            conn.execute(
                """INSERT OR IGNORE INTO dim_tiempo
                   (fecha_id, anio, mes, dia, trimestre, nombre_mes, dia_semana)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (fecha_str, d.year, d.month, d.day, trimestre, nombre_mes, dia_semana),
            )
            loaded += 1
        except ValueError:
            continue

    conn.commit()
    logger.info("  dim_tiempo: %d fechas cargadas", loaded)


def _load_dim_contribuyente(conn: sqlite3.Connection, logger: logging.Logger) -> dict[str, int]:
    """Carga dimensión contribuyentes desde clean/contribuyentes_limpios.csv."""
    clean_contrib_file = config.CLEAN_DIR / "contribuyentes_limpios.csv"
    if not clean_contrib_file.exists():
        logger.warning("  No existe %s", clean_contrib_file.name)
        return {}

    rfc_map: dict[str, int] = {}
    with open(clean_contrib_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rfc = row["rfc"].strip().upper()
            cursor = conn.execute(
                """INSERT OR IGNORE INTO dim_contribuyente
                   (rfc, nombre, tipo_persona, nota_sat, longitud_rfc)
                   VALUES (?, ?, ?, ?, ?)""",
                (rfc, row["nombre"], row["tipo_persona"], row.get("nota_sat", ""), len(rfc)),
            )
            if cursor.lastrowid:
                rfc_map[rfc] = cursor.lastrowid
            else:
                cur = conn.execute("SELECT contribuyente_id FROM dim_contribuyente WHERE rfc = ?", (rfc,))
                res = cur.fetchone()
                if res:
                    rfc_map[rfc] = res[0]

    conn.commit()
    logger.info("  dim_contribuyente: %d contribuyentes cargados", len(rfc_map))
    return rfc_map


def _load_fact_publicaciones(
    conn: sqlite3.Connection, rfc_map: dict[str, int], logger: logging.Logger
) -> int:
    """Carga tabla de hechos desde clean/publicaciones_long.csv."""
    pub_long_file = config.CLEAN_DIR / "publicaciones_long.csv"
    if not pub_long_file.exists():
        logger.warning("  No existe %s", pub_long_file.name)
        return 0

    loaded = 0
    with open(pub_long_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rfc = row["rfc"].strip().upper()
            contrib_id = rfc_map.get(rfc)
            if not contrib_id:
                continue

            etapa = row["situacion_69b"].strip()
            situacion_id = config.SITUACIONES_CATALOGO.get(etapa, 1)

            medio = row["medio"].strip()
            num_oficio = row.get("numero_oficio", "").strip()
            fecha_oficio = row.get("fecha_oficio", "").strip() or None
            fecha_pub = row.get("fecha_publicacion", "").strip() or None

            fecha_pres = fecha_pub if etapa == "Presunto" else None
            fecha_def = fecha_pub if etapa == "Definitivo" else None
            fecha_sent = fecha_pub if etapa == "Sentencia Favorable" else None
            fecha_desv = fecha_pub if etapa == "Desvirtuado" else None

            num_of_pres = num_oficio if etapa == "Presunto" else ""
            num_of_def = num_oficio if etapa == "Definitivo" else ""

            row_key = f"{rfc}|{etapa}|{medio}|{num_oficio}|{fecha_pub}".encode("utf-8")
            row_hash = hashlib.md5(row_key).hexdigest()

            conn.execute(
                """INSERT OR IGNORE INTO fact_listado_69b
                   (row_hash, contribuyente_id, situacion_id, medio, numero_oficio,
                    fecha_oficio_id, fecha_publicacion_id, fecha_presuncion_id,
                    fecha_definitivo_id, fecha_sentencia_id, fecha_desvirtuado_id,
                    num_oficio_presuncion, num_oficio_definitivos, pipeline_ts)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    row_hash, contrib_id, situacion_id, medio, num_oficio,
                    fecha_oficio, fecha_pub, fecha_pres, fecha_def, fecha_sent,
                    fecha_desv, num_of_pres, num_of_def, config.PIPELINE_RUN_TS
                ),
            )
            loaded += 1

    conn.commit()
    logger.info("  fact_listado_69b: %d eventos cargados", loaded)
    return loaded


# ──────────────────────────────────────────────
# Punto de entrada de la fase LOAD DW
# ──────────────────────────────────────────────

def run_load_dw(logger: logging.Logger) -> dict[str, Any]:
    """Carga limpia y estandarizada al Data Warehouse."""
    logger.info("=" * 60)
    logger.info("FASE 4: LOAD DATA WAREHOUSE (Esquema Estrella en dw_sat.db)")
    logger.info("=" * 60)

    conn = _init_database(config.DW_DB_PATH, logger)
    total_facts = 0

    try:
        _load_dim_situacion(conn, logger)

        # Recolectar fechas
        fechas: set[str] = set()
        pub_long_file = config.CLEAN_DIR / "publicaciones_long.csv"
        if pub_long_file.exists():
            with open(pub_long_file, "r", encoding="utf-8") as f:
                for r in csv.DictReader(f):
                    if r.get("fecha_publicacion"):
                        fechas.add(r["fecha_publicacion"].strip())
                    if r.get("fecha_oficio"):
                        fechas.add(r["fecha_oficio"].strip())

        padron_clean = config.CLEAN_DIR / "padron_universo_limpio.csv"
        if padron_clean.exists():
            with open(padron_clean, "r", encoding="utf-8") as f:
                for r in csv.DictReader(f):
                    if r.get("fecha_corte"):
                        fechas.add(r["fecha_corte"].strip())

        _load_dim_tiempo(conn, fechas, logger)
        rfc_map = _load_dim_contribuyente(conn, logger)
        total_facts = _load_fact_publicaciones(conn, rfc_map, logger)

        conn.execute(
            """INSERT INTO meta_pipeline_runs (run_ts, phase, records_loaded, status)
               VALUES (?, 'load_dw', ?, 'OK')""",
            (config.PIPELINE_RUN_TS, total_facts),
        )
        conn.commit()

    finally:
        conn.close()

    logger.info("Carga al Data Warehouse completada exitosamente.")
    return {
        "db_path": str(config.DW_DB_PATH),
        "total_facts": total_facts,
        "status": "OK",
    }
