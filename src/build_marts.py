"""
src/build_marts.py — Fase MARTS del pipeline
==============================================
Construye Data Marts analíticos a partir del Data Warehouse.
Los marts son vistas materializadas (tablas) en una DB SQLite
separada, optimizadas para consultas de análisis y dashboards.

Marts generados:
  1. mart_riesgo_fiscal     → Score de riesgo por contribuyente
  2. mart_por_situacion     → Concentrado por tipo de situación
  3. mart_tendencia_temporal → Tendencia mensual/anual de publicaciones
  4. mart_tipo_persona      → Distribución por tipo de persona (física/moral)
"""

import logging
import sqlite3
from pathlib import Path
from typing import Any

import config


# ──────────────────────────────────────────────
# Consultas SQL para construir los marts
# ──────────────────────────────────────────────

MART_RIESGO_FISCAL = """
CREATE TABLE IF NOT EXISTS mart_riesgo_fiscal AS
SELECT
    c.rfc,
    c.nombre,
    c.tipo_persona,
    s.situacion_nombre        AS situacion,
    s.nivel_riesgo,
    f.fecha_presuncion_id     AS fecha_presuncion,
    f.fecha_definitivo_id     AS fecha_definitivo,
    f.fecha_sentencia_id      AS fecha_sentencia,
    f.fecha_desvirtuado_id    AS fecha_desvirtuado,
    f.num_oficio_presuncion,
    f.num_oficio_definitivos,
    -- Score de riesgo (1-100)
    CASE s.nivel_riesgo
        WHEN 'ALTO'  THEN 85
        WHEN 'MEDIO' THEN 50
        WHEN 'BAJO'  THEN 20
        WHEN 'NULO'  THEN 5
        ELSE 0
    END AS score_riesgo,
    -- Flag binario de riesgo alto
    CASE WHEN s.nivel_riesgo IN ('ALTO') THEN 1 ELSE 0 END AS flag_riesgo_alto,
    f.pipeline_ts
FROM fact_listado_69b f
JOIN dim_contribuyente c ON f.contribuyente_id = c.contribuyente_id
JOIN dim_situacion s     ON f.situacion_id = s.situacion_id
ORDER BY score_riesgo DESC, c.rfc;
"""

MART_POR_SITUACION = """
CREATE TABLE IF NOT EXISTS mart_por_situacion AS
SELECT
    s.situacion_nombre        AS situacion,
    s.nivel_riesgo,
    s.descripcion,
    COUNT(*)                  AS total_contribuyentes,
    SUM(CASE WHEN c.tipo_persona = 'MORAL'  THEN 1 ELSE 0 END) AS personas_morales,
    SUM(CASE WHEN c.tipo_persona = 'FISICA' THEN 1 ELSE 0 END) AS personas_fisicas,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM fact_listado_69b), 2)
                              AS porcentaje_del_total
FROM fact_listado_69b f
JOIN dim_situacion s     ON f.situacion_id = s.situacion_id
JOIN dim_contribuyente c ON f.contribuyente_id = c.contribuyente_id
GROUP BY s.situacion_nombre, s.nivel_riesgo, s.descripcion
ORDER BY total_contribuyentes DESC;
"""

MART_TENDENCIA_TEMPORAL = """
CREATE TABLE IF NOT EXISTS mart_tendencia_temporal AS
SELECT
    t.anio,
    t.mes,
    t.nombre_mes,
    t.trimestre,
    s.situacion_nombre AS situacion,
    COUNT(*)           AS publicaciones
FROM fact_listado_69b f
JOIN dim_tiempo t    ON f.fecha_presuncion_id = t.fecha_id
JOIN dim_situacion s ON f.situacion_id = s.situacion_id
WHERE f.fecha_presuncion_id IS NOT NULL
GROUP BY t.anio, t.mes, t.nombre_mes, t.trimestre, s.situacion_nombre
ORDER BY t.anio, t.mes;
"""

MART_TIPO_PERSONA = """
CREATE TABLE IF NOT EXISTS mart_tipo_persona AS
SELECT
    c.tipo_persona,
    s.situacion_nombre AS situacion,
    s.nivel_riesgo,
    COUNT(*)           AS total,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM fact_listado_69b), 2)
                       AS porcentaje
FROM fact_listado_69b f
JOIN dim_contribuyente c ON f.contribuyente_id = c.contribuyente_id
JOIN dim_situacion s     ON f.situacion_id = s.situacion_id
GROUP BY c.tipo_persona, s.situacion_nombre, s.nivel_riesgo
ORDER BY c.tipo_persona, total DESC;
"""


# ──────────────────────────────────────────────
# Construcción de marts
# ──────────────────────────────────────────────

def _init_marts_db(logger: logging.Logger) -> sqlite3.Connection:
    """Inicializa la base de datos de marts (recarga idempotente)."""
    if config.MARTS_DB_PATH.exists():
        config.MARTS_DB_PATH.unlink()
        logger.info("  Marts DB anterior eliminada.")

    conn = sqlite3.connect(str(config.MARTS_DB_PATH))
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    return conn


def _attach_dw(conn: sqlite3.Connection, logger: logging.Logger) -> None:
    """Adjunta la base de datos del DW para leer sus tablas."""
    dw_path = str(config.DW_DB_PATH).replace("\\", "/")
    conn.execute(f"ATTACH DATABASE '{dw_path}' AS dw;")
    logger.info("  DW adjuntado desde: %s", config.DW_DB_PATH.name)


def _build_mart(
    conn: sqlite3.Connection,
    name: str,
    sql: str,
    logger: logging.Logger,
) -> int:
    """Construye un mart individual y retorna conteo de filas."""
    # Reemplazar nombres de tablas para usar el esquema adjunto 'dw.'
    sql_with_schema = sql
    for table in [
        "fact_listado_69b", "dim_contribuyente",
        "dim_situacion", "dim_tiempo",
    ]:
        sql_with_schema = sql_with_schema.replace(table, f"dw.{table}")

    conn.executescript(f"DROP TABLE IF EXISTS {name}; {sql_with_schema}")
    cursor = conn.execute(f"SELECT COUNT(*) FROM {name};")
    count = cursor.fetchone()[0]
    logger.info("  ✔ %s: %d registros", name, count)
    return count


# ──────────────────────────────────────────────
# Punto de entrada de la fase
# ──────────────────────────────────────────────
def run_build_marts(logger: logging.Logger) -> dict[str, Any]:
    """Ejecuta la construcción de todos los Data Marts."""
    logger.info("Iniciando construcción de Data Marts…")

    if not config.DW_DB_PATH.exists():
        logger.error("  DW no encontrado: %s", config.DW_DB_PATH)
        raise FileNotFoundError(f"DW no encontrado: {config.DW_DB_PATH}")

    conn = _init_marts_db(logger)
    marts_built: dict[str, int] = {}

    try:
        _attach_dw(conn, logger)

        marts = [
            ("mart_riesgo_fiscal",      MART_RIESGO_FISCAL),
            ("mart_por_situacion",      MART_POR_SITUACION),
            ("mart_tendencia_temporal", MART_TENDENCIA_TEMPORAL),
            ("mart_tipo_persona",       MART_TIPO_PERSONA),
        ]

        for name, sql in marts:
            count = _build_mart(conn, name, sql, logger)
            marts_built[name] = count

    finally:
        conn.close()

    logger.info("Marts completados: %d tablas construidas en %s",
                 len(marts_built), config.MARTS_DB_PATH.name)
    return {
        "db_path": str(config.MARTS_DB_PATH),
        "marts": marts_built,
        "status": "OK",
    }
