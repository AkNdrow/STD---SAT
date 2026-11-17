"""
main.py — Orquestador del pipeline ETL SAT 69-B
=================================================
Ejecuta las fases del pipeline en orden:
  1. EXTRACT   → Descarga datos crudos a raw/
  2. PROFILE   → Diagnóstico de calidad de datos
  3. TRANSFORM → Limpieza y normalización a clean/
  4. LOAD DW   → Carga al Data Warehouse (SQLite) en dw/
  5. MARTS     → Construcción de Data Marts en marts/
  6. DASHBOARD → Generación de dashboard HTML en dashboard/

Uso:
  python main.py                                        # Pipeline completo (6 fases)
  python main.py --extract --profile --transform        # Solo fases 1-3
  python main.py extract profile transform              # Equivalente (sin --)
  python main.py --extract                              # Solo extracción
  python main.py --help                                 # Ayuda

Ejecución idempotente: cada corrida recrea las capas desde cero.
"""

import sys
import time
import logging
from datetime import datetime, timezone

# ── Configuración ─────────────────────────────
import config
from src.extract import run_extract
from src.profile import run_profile
from src.transform import run_transform
from src.load_dw import run_load_dw
from src.build_marts import run_build_marts
from src.generate_dashboard import run_generate_dashboard


# ──────────────────────────────────────────────
# Logger
# ──────────────────────────────────────────────
def _setup_logger() -> logging.Logger:
    """Configura logging a consola y archivo."""
    logger = logging.getLogger("etl_sat_69b")
    # Evitar duplicados si se ejecuta más de una vez en la misma sesión
    if logger.handlers:
        return logger
    logger.setLevel(getattr(logging, config.LOG_LEVEL, logging.INFO))

    fmt = logging.Formatter(
        "[%(asctime)s] %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    fh = logging.FileHandler(config.LOG_FILE, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger


# ──────────────────────────────────────────────
# Registro de fases
# ──────────────────────────────────────────────
PHASES = {
    "extract":   ("1/6 EXTRACT   — Descarga de datos crudos",   run_extract),
    "profile":   ("2/6 PROFILE   — Perfilado de calidad",       run_profile),
    "transform": ("3/6 TRANSFORM — Limpieza y normalización",   run_transform),
    "load":      ("4/6 LOAD DW   — Carga al Data Warehouse",    run_load_dw),
    "marts":     ("5/6 MARTS     — Construcción de Data Marts",  run_build_marts),
    "dashboard": ("6/6 DASHBOARD — Generación de reportes",     run_generate_dashboard),
}

ALL_PHASE_KEYS = list(PHASES.keys())


def _print_banner(logger: logging.Logger) -> None:
    banner = r"""
    ╔══════════════════════════════════════════════════════╗
    ║   ETL SAT 69-B  —  Pipeline de Riesgo Fiscal        ║
    ║   Software para la Toma de Decisiones                ║
    ╚══════════════════════════════════════════════════════╝
    """
    logger.info(banner)
    logger.info("Inicio del pipeline: %s", config.PIPELINE_RUN_TS)


def _print_help() -> None:
    print(__doc__)
    print("Fases disponibles:")
    for key, (desc, _) in PHASES.items():
        print(f"  --{key:12s}  →  {desc}")
    print("\nEjemplos:")
    print("  python main.py --extract --profile --transform")
    print("  python main.py extract transform")
    print("  python main.py                   # todas las fases")
    print()


def _parse_phases(argv: list[str]) -> list[str]:
    """
    Parsea argumentos de línea de comandos.
    Acepta tanto '--extract' como 'extract' (sin guiones).
    """
    if not argv:
        return ALL_PHASE_KEYS

    phases: list[str] = []
    for arg in argv:
        # Normalizar: quitar -- y convertir a minúsculas
        clean = arg.lstrip("-").lower().strip()
        if clean in ("help", "h"):
            return ["__help__"]
        if clean in PHASES:
            phases.append(clean)
        else:
            return ["__unknown__", arg]

    return phases if phases else ALL_PHASE_KEYS


# ──────────────────────────────────────────────
# Ejecución principal
# ──────────────────────────────────────────────
def main() -> None:
    logger = _setup_logger()

    phases_to_run = _parse_phases(sys.argv[1:])

    if phases_to_run and phases_to_run[0] == "__help__":
        _print_help()
        return

    if phases_to_run and phases_to_run[0] == "__unknown__":
        logger.error("Fase desconocida: '%s'. Usa --help.", phases_to_run[1])
        sys.exit(1)

    _print_banner(logger)
    logger.info("Fases a ejecutar: %s", ", ".join(phases_to_run))
    logger.info("=" * 60)

    total_start = time.time()
    results: dict[str, dict] = {}

    for phase_key in phases_to_run:
        desc, func = PHASES[phase_key]
        logger.info("▶ Iniciando: %s", desc)
        phase_start = time.time()

        try:
            result = func(logger)
            elapsed = time.time() - phase_start
            results[phase_key] = {
                "status": "OK",
                "elapsed_secs": round(elapsed, 2),
                "detail": result,
            }
            logger.info("✔ Completada: %s (%.2f s)", phase_key, elapsed)
        except Exception as exc:
            elapsed = time.time() - phase_start
            results[phase_key] = {
                "status": "ERROR",
                "elapsed_secs": round(elapsed, 2),
                "detail": str(exc),
            }
            logger.error("✘ Error en %s: %s (%.2f s)", phase_key, exc, elapsed)
            logger.exception("Traceback completo:")
            logger.warning("Abortando pipeline por error en fase '%s'.", phase_key)
            break

        logger.info("-" * 60)

    # ── Resumen final ──
    total_elapsed = time.time() - total_start
    logger.info("=" * 60)
    logger.info("RESUMEN DEL PIPELINE")
    logger.info("=" * 60)
    for key, res in results.items():
        icon = "✔" if res["status"] == "OK" else "✘"
        logger.info("  %s %-12s  %s  (%.2f s)", icon, key, res["status"], res["elapsed_secs"])
    logger.info("-" * 60)
    logger.info("Tiempo total: %.2f s", total_elapsed)
    logger.info("Fin del pipeline: %s", datetime.now(timezone.utc).isoformat(timespec="seconds"))

    any_error = any(r["status"] == "ERROR" for r in results.values())
    sys.exit(1 if any_error else 0)


if __name__ == "__main__":
    main()
