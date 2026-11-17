"""
src/generate_dashboard.py — Fase DASHBOARD del pipeline
========================================================
Genera un dashboard HTML estático auto-contenido a partir
de los Data Marts en SQLite. No requiere servidor web:
se abre directamente en el navegador.

Componentes del dashboard:
  - KPIs principales (tarjetas)
  - Tabla de riesgo fiscal
  - Distribución por situación (tabla de resumen)
  - Distribución por tipo de persona
  - Metadatos de la corrida del pipeline
"""

import logging
import sqlite3
from pathlib import Path
from typing import Any

import config


def _query_marts(db_path: Path, sql: str) -> list[tuple]:
    """Ejecuta una consulta sobre la DB de marts."""
    if not db_path.exists():
        return []
    conn = sqlite3.connect(str(db_path))
    try:
        cursor = conn.execute(sql)
        return cursor.fetchall()
    finally:
        conn.close()


def _query_marts_with_headers(db_path: Path, sql: str) -> tuple[list[str], list[tuple]]:
    """Ejecuta consulta y retorna headers + rows."""
    if not db_path.exists():
        return [], []
    conn = sqlite3.connect(str(db_path))
    try:
        cursor = conn.execute(sql)
        headers = [desc[0] for desc in cursor.description] if cursor.description else []
        rows = cursor.fetchall()
        return headers, rows
    finally:
        conn.close()


def _build_kpis(db_path: Path) -> dict[str, Any]:
    """Calcula KPIs principales."""
    kpis = {}

    # Total de contribuyentes en lista
    rows = _query_marts(db_path, "SELECT COUNT(*) FROM mart_riesgo_fiscal;")
    kpis["total_contribuyentes"] = rows[0][0] if rows else 0

    # Riesgo alto
    rows = _query_marts(db_path,
        "SELECT COUNT(*) FROM mart_riesgo_fiscal WHERE flag_riesgo_alto = 1;")
    kpis["riesgo_alto"] = rows[0][0] if rows else 0

    # Personas morales vs físicas
    rows = _query_marts(db_path,
        "SELECT tipo_persona, COUNT(*) FROM mart_riesgo_fiscal GROUP BY tipo_persona;")
    for row in rows:
        if row[0] == "MORAL":
            kpis["personas_morales"] = row[1]
        elif row[0] == "FISICA":
            kpis["personas_fisicas"] = row[1]

    kpis.setdefault("personas_morales", 0)
    kpis.setdefault("personas_fisicas", 0)

    # Score promedio
    rows = _query_marts(db_path,
        "SELECT ROUND(AVG(score_riesgo), 1) FROM mart_riesgo_fiscal;")
    kpis["score_promedio"] = rows[0][0] if rows and rows[0][0] else 0

    return kpis


def _build_html(kpis: dict, db_path: Path) -> str:
    """Genera el HTML completo del dashboard."""

    # ── Tabla de riesgo fiscal ──
    headers, risk_rows = _query_marts_with_headers(db_path,
        """SELECT rfc, nombre, tipo_persona, situacion, nivel_riesgo, score_riesgo
           FROM mart_riesgo_fiscal ORDER BY score_riesgo DESC LIMIT 50;""")

    risk_table_rows = ""
    for row in risk_rows:
        rfc, nombre, tipo, sit, nivel, score = row
        badge_class = {
            "ALTO": "badge-danger",
            "MEDIO": "badge-warning",
            "BAJO": "badge-success",
            "NULO": "badge-secondary",
        }.get(nivel, "badge-secondary")

        risk_table_rows += f"""
        <tr>
            <td><code>{rfc}</code></td>
            <td>{nombre}</td>
            <td>{tipo}</td>
            <td>{sit}</td>
            <td><span class="badge {badge_class}">{nivel}</span></td>
            <td class="text-center"><strong>{score}</strong></td>
        </tr>"""

    # ── Tabla de situaciones ──
    _, sit_rows = _query_marts_with_headers(db_path,
        "SELECT * FROM mart_por_situacion ORDER BY total_contribuyentes DESC;")

    sit_table_rows = ""
    for row in sit_rows:
        sit_table_rows += "<tr>" + "".join(f"<td>{v}</td>" for v in row) + "</tr>"

    # ── Tabla tipo persona ──
    _, tp_rows = _query_marts_with_headers(db_path,
        "SELECT * FROM mart_tipo_persona ORDER BY total DESC;")

    tp_table_rows = ""
    for row in tp_rows:
        tp_table_rows += "<tr>" + "".join(f"<td>{v}</td>" for v in row) + "</tr>"

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Dashboard de Riesgo Fiscal — SAT Art. 69-B</title>
    <style>
        :root {{
            --primary: #1a365d;
            --danger: #e53e3e;
            --warning: #dd6b20;
            --success: #38a169;
            --secondary: #718096;
            --bg: #f7fafc;
            --card-bg: #ffffff;
        }}
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: 'Segoe UI', system-ui, -apple-system, sans-serif;
            background: var(--bg);
            color: #2d3748;
            line-height: 1.6;
        }}
        .header {{
            background: linear-gradient(135deg, var(--primary), #2c5282);
            color: white;
            padding: 2rem;
            text-align: center;
        }}
        .header h1 {{ font-size: 1.8rem; margin-bottom: 0.5rem; }}
        .header p {{ opacity: 0.85; font-size: 0.95rem; }}
        .container {{ max-width: 1200px; margin: 0 auto; padding: 1.5rem; }}

        /* KPIs */
        .kpi-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1rem;
            margin-bottom: 2rem;
        }}
        .kpi-card {{
            background: var(--card-bg);
            border-radius: 12px;
            padding: 1.5rem;
            text-align: center;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            border-top: 4px solid var(--primary);
        }}
        .kpi-card.danger {{ border-top-color: var(--danger); }}
        .kpi-card .kpi-value {{
            font-size: 2.5rem;
            font-weight: 700;
            color: var(--primary);
        }}
        .kpi-card.danger .kpi-value {{ color: var(--danger); }}
        .kpi-card .kpi-label {{
            font-size: 0.85rem;
            color: var(--secondary);
            margin-top: 0.3rem;
        }}

        /* Tables */
        .section {{ margin-bottom: 2rem; }}
        .section h2 {{
            font-size: 1.3rem;
            color: var(--primary);
            margin-bottom: 1rem;
            padding-bottom: 0.5rem;
            border-bottom: 2px solid #e2e8f0;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            background: var(--card-bg);
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            font-size: 0.9rem;
        }}
        th {{
            background: var(--primary);
            color: white;
            padding: 0.75rem 1rem;
            text-align: left;
            font-weight: 600;
        }}
        td {{ padding: 0.6rem 1rem; border-bottom: 1px solid #e2e8f0; }}
        tr:hover {{ background: #edf2f7; }}
        code {{ background: #edf2f7; padding: 2px 6px; border-radius: 4px; font-size: 0.85rem; }}

        /* Badges */
        .badge {{
            padding: 3px 10px;
            border-radius: 12px;
            font-size: 0.75rem;
            font-weight: 600;
            color: white;
        }}
        .badge-danger {{ background: var(--danger); }}
        .badge-warning {{ background: var(--warning); }}
        .badge-success {{ background: var(--success); }}
        .badge-secondary {{ background: var(--secondary); }}
        .text-center {{ text-align: center; }}

        /* Footer */
        .footer {{
            text-align: center;
            padding: 1.5rem;
            color: var(--secondary);
            font-size: 0.8rem;
            border-top: 1px solid #e2e8f0;
            margin-top: 2rem;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>📊 Dashboard de Riesgo Fiscal</h1>
        <p>Análisis del Listado Art. 69-B del CFF — Contribuyentes con Operaciones Simuladas (EFOS)</p>
        <p style="font-size:0.8rem; margin-top:0.5rem;">
            Pipeline ejecutado: {config.PIPELINE_RUN_TS} |
            Software para la Toma de Decisiones
        </p>
    </div>

    <div class="container">
        <!-- KPIs -->
        <div class="kpi-grid">
            <div class="kpi-card">
                <div class="kpi-value">{kpis['total_contribuyentes']}</div>
                <div class="kpi-label">Total en Lista 69-B</div>
            </div>
            <div class="kpi-card danger">
                <div class="kpi-value">{kpis['riesgo_alto']}</div>
                <div class="kpi-label">Riesgo Alto</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-value">{kpis['personas_morales']}</div>
                <div class="kpi-label">Personas Morales</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-value">{kpis['personas_fisicas']}</div>
                <div class="kpi-label">Personas Físicas</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-value">{kpis['score_promedio']}</div>
                <div class="kpi-label">Score Riesgo Promedio</div>
            </div>
        </div>

        <!-- Tabla de riesgo fiscal -->
        <div class="section">
            <h2>🔍 Detalle de Riesgo por Contribuyente</h2>
            <table>
                <thead>
                    <tr>
                        <th>RFC</th>
                        <th>Nombre</th>
                        <th>Tipo</th>
                        <th>Situación</th>
                        <th>Nivel Riesgo</th>
                        <th>Score</th>
                    </tr>
                </thead>
                <tbody>
                    {risk_table_rows if risk_table_rows else '<tr><td colspan="6" class="text-center">Sin datos</td></tr>'}
                </tbody>
            </table>
        </div>

        <!-- Distribución por situación -->
        <div class="section">
            <h2>📋 Distribución por Situación</h2>
            <table>
                <thead>
                    <tr>
                        <th>Situación</th>
                        <th>Nivel Riesgo</th>
                        <th>Descripción</th>
                        <th>Total</th>
                        <th>P. Morales</th>
                        <th>P. Físicas</th>
                        <th>%</th>
                    </tr>
                </thead>
                <tbody>
                    {sit_table_rows if sit_table_rows else '<tr><td colspan="7" class="text-center">Sin datos</td></tr>'}
                </tbody>
            </table>
        </div>

        <!-- Distribución por tipo persona -->
        <div class="section">
            <h2>👥 Distribución por Tipo de Persona</h2>
            <table>
                <thead>
                    <tr>
                        <th>Tipo Persona</th>
                        <th>Situación</th>
                        <th>Nivel Riesgo</th>
                        <th>Total</th>
                        <th>%</th>
                    </tr>
                </thead>
                <tbody>
                    {tp_table_rows if tp_table_rows else '<tr><td colspan="5" class="text-center">Sin datos</td></tr>'}
                </tbody>
            </table>
        </div>
    </div>

    <div class="footer">
        <p>ETL SAT 69-B — Proyecto académico: Software para la Toma de Decisiones</p>
        <p>Datos fuente: datos.gob.mx | Motor: Python 3.10+ + SQLite | Sin dependencias externas</p>
    </div>
</body>
</html>"""
    return html


# ──────────────────────────────────────────────
# Punto de entrada de la fase
# ──────────────────────────────────────────────
def run_generate_dashboard(logger: logging.Logger) -> dict[str, Any]:
    """Genera el dashboard HTML estático."""
    logger.info("Iniciando generación del dashboard…")

    if not config.MARTS_DB_PATH.exists():
        logger.error("  Marts DB no encontrada: %s", config.MARTS_DB_PATH)
        raise FileNotFoundError(f"Marts DB no encontrada: {config.MARTS_DB_PATH}")

    # Calcular KPIs
    kpis = _build_kpis(config.MARTS_DB_PATH)
    logger.info("  KPIs calculados: %s", kpis)

    # Generar HTML
    html = _build_html(kpis, config.MARTS_DB_PATH)

    # Guardar
    output_path = config.DASH_DIR / "dashboard_riesgo_fiscal.html"
    output_path.write_text(html, encoding="utf-8")
    logger.info("  Dashboard generado: %s (%.1f KB)",
                 output_path.name, output_path.stat().st_size / 1024)

    return {
        "file": str(output_path),
        "size_kb": round(output_path.stat().st_size / 1024, 1),
        "kpis": kpis,
        "status": "OK",
    }
