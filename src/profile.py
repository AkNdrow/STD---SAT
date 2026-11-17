"""
src/profile.py — Fase PROFILE del pipeline ETL SAT 69-B
========================================================
Analiza TODOS los CSV en raw/ y genera un reporte de calidad
de datos en clean/perfilado_fuentes.md (y salida en consola).

Por cada archivo reporta:
  - Codificación detectada
  - Total de filas (sin contar header)
  - Nombres exactos de columnas
  - Conteo y porcentaje de valores nulos/vacíos por columna
  - Ejemplos de formatos de fecha encontrados

Solo biblioteca estándar: csv, re, pathlib, json.
"""

import csv
import json
import logging
import re
from collections import Counter
from pathlib import Path
from typing import Any

import config
from src.extract import detect_encoding  # reutilizar la heurística


# ──────────────────────────────────────────────
# Patrones de fecha para detección automática
# ──────────────────────────────────────────────

DATE_PATTERNS: list[tuple[str, re.Pattern]] = [
    ("DD/MM/YYYY",          re.compile(r"\b\d{1,2}/\d{1,2}/\d{4}\b")),
    ("YYYY-MM-DD",          re.compile(r"\b\d{4}-\d{2}-\d{2}\b")),
    ("DD-MM-YYYY",          re.compile(r"\b\d{1,2}-\d{1,2}-\d{4}\b")),
    ("D de mes de YYYY",    re.compile(
        r"\b\d{1,2}\s+de\s+(?:enero|febrero|marzo|abril|mayo|junio|"
        r"julio|agosto|septiembre|octubre|noviembre|diciembre)\s+de\s+\d{4}\b",
        re.IGNORECASE,
    )),
    ("YYYY-MM",             re.compile(r"\b\d{4}-\d{2}\b")),
    ("Periodo YYYYMM",      re.compile(r"\b20\d{4}\b")),  # e.g. 202301
]


def _read_csv_safe(filepath: Path) -> tuple[str, list[str], list[list[str]]]:
    """
    Lee un CSV detectando codificación automáticamente.
    Retorna (encoding, headers, rows_as_lists).
    """
    enc = detect_encoding(filepath)

    with open(filepath, "r", encoding=enc, errors="replace") as f:
        # Intentar detectar delimitador
        sample = f.read(4096)
        f.seek(0)

        # Heurística simple de delimitador
        delim = ","
        if sample.count("|") > sample.count(",") and sample.count("|") > 5:
            delim = "|"
        elif sample.count("\t") > sample.count(",") and sample.count("\t") > 5:
            delim = "\t"

        reader = csv.reader(f, delimiter=delim)
        try:
            headers = next(reader)
        except StopIteration:
            return enc, [], []
        rows = list(reader)

    return enc, headers, rows


def _profile_one_csv(
    filepath: Path, logger: logging.Logger
) -> dict[str, Any]:
    """
    Genera perfil completo de un archivo CSV.
    """
    enc, headers, rows = _read_csv_safe(filepath)
    total_rows = len(rows)
    num_cols = len(headers)

    logger.info("  ── %s ──", filepath.name)
    logger.info("    Codificación: %s", enc)
    logger.info("    Filas: %d  |  Columnas: %d", total_rows, num_cols)

    # ── Análisis de nulos por columna ──
    col_stats: list[dict[str, Any]] = []
    for idx, col_name in enumerate(headers):
        null_count = 0
        for row in rows:
            val = row[idx].strip() if idx < len(row) else ""
            if not val:
                null_count += 1

        pct = round(null_count / total_rows * 100, 1) if total_rows else 0
        col_stats.append({
            "columna": col_name.strip(),
            "indice": idx,
            "nulos": null_count,
            "no_nulos": total_rows - null_count,
            "pct_nulo": pct,
        })

    # ── Detección de formatos de fecha ──
    date_examples: dict[str, list[str]] = {}
    all_text = "\n".join(
        ",".join(row) for row in rows[:200]  # muestra de 200 filas
    )
    for fmt_name, pattern in DATE_PATTERNS:
        matches = pattern.findall(all_text)
        if matches:
            # Tomar hasta 3 ejemplos únicos
            unique = list(dict.fromkeys(matches))[:3]
            date_examples[fmt_name] = unique

    profile = {
        "archivo": filepath.name,
        "ruta": str(filepath),
        "codificacion": enc,
        "total_filas": total_rows,
        "total_columnas": num_cols,
        "columnas": [cs["columna"] for cs in col_stats],
        "nulos_por_columna": col_stats,
        "formatos_fecha_detectados": date_examples,
    }

    # Log resumen
    for cs in col_stats:
        bar = "█" * int(cs["pct_nulo"] / 5) if cs["pct_nulo"] > 0 else "░"
        logger.info("    %-45s  nulos: %4d / %4d  (%5.1f%%)  %s",
                     cs["columna"][:45], cs["nulos"], total_rows, cs["pct_nulo"], bar)

    if date_examples:
        logger.info("    Formatos de fecha detectados:")
        for fmt_name, examples in date_examples.items():
            logger.info("      %-25s → %s", fmt_name, ", ".join(examples[:3]))

    return profile


def _write_markdown_report(
    profiles: list[dict], logger: logging.Logger
) -> Path:
    """
    Genera clean/perfilado_fuentes.md con el reporte completo.
    """
    out_path = config.CLEAN_DIR / "perfilado_fuentes.md"

    lines: list[str] = []
    lines.append("# Reporte de Perfilado de Fuentes de Datos")
    lines.append("")
    lines.append(f"> Generado automáticamente: `{config.PIPELINE_RUN_TS}`")
    lines.append("")
    lines.append(f"**Archivos analizados:** {len(profiles)}")
    lines.append("")

    for prof in profiles:
        lines.append(f"## {prof['archivo']}")
        lines.append("")
        lines.append(f"| Propiedad | Valor |")
        lines.append(f"|-----------|-------|")
        lines.append(f"| Codificación | `{prof['codificacion']}` |")
        lines.append(f"| Total de filas | {prof['total_filas']:,} |")
        lines.append(f"| Total de columnas | {prof['total_columnas']} |")
        lines.append("")

        # Tabla de columnas
        lines.append("### Análisis de completitud por columna")
        lines.append("")
        lines.append("| # | Columna | Nulos | No nulos | % Nulo |")
        lines.append("|---|---------|-------|----------|--------|")
        for cs in prof["nulos_por_columna"]:
            lines.append(
                f"| {cs['indice']} | `{cs['columna']}` | "
                f"{cs['nulos']:,} | {cs['no_nulos']:,} | {cs['pct_nulo']:.1f}% |"
            )
        lines.append("")

        # Formatos de fecha
        if prof["formatos_fecha_detectados"]:
            lines.append("### Formatos de fecha detectados")
            lines.append("")
            lines.append("| Formato | Ejemplos |")
            lines.append("|---------|----------|")
            for fmt_name, examples in prof["formatos_fecha_detectados"].items():
                lines.append(f"| `{fmt_name}` | {', '.join(f'`{e}`' for e in examples)} |")
            lines.append("")

        lines.append("---")
        lines.append("")

    out_path.write_text("\n".join(lines), encoding="utf-8")
    logger.info("  Reporte Markdown guardado: %s", out_path.name)
    return out_path


# ──────────────────────────────────────────────
# Punto de entrada de la fase
# ──────────────────────────────────────────────

def run_profile(logger: logging.Logger) -> dict[str, Any]:
    """Ejecuta el perfilado de todos los CSVs en raw/."""
    logger.info("=" * 50)
    logger.info("FASE PROFILE — Perfilado de calidad de datos")
    logger.info("=" * 50)

    csv_files = sorted(config.RAW_DIR.glob("*.csv"))

    if not csv_files:
        logger.warning("  No se encontraron CSVs en raw/.")
        return {"profiles": [], "report_path": None}

    logger.info("  Archivos CSV encontrados: %d", len(csv_files))
    profiles: list[dict] = []

    for csv_file in csv_files:
        try:
            prof = _profile_one_csv(csv_file, logger)
            profiles.append(prof)
        except Exception as exc:
            logger.error("  Error perfilando %s: %s", csv_file.name, exc)

    # Guardar reporte Markdown
    report_path = _write_markdown_report(profiles, logger)

    # Guardar también como JSON para consumo programático
    json_path = config.CLEAN_DIR / "perfilado_fuentes.json"
    json_path.write_text(
        json.dumps(profiles, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    logger.info("Perfilado completado: %d archivos analizados.", len(profiles))
    return {
        "files_profiled": len(profiles),
        "report_md": str(report_path),
        "report_json": str(json_path),
    }
