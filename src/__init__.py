# src/ — Paquete principal del pipeline ETL SAT 69-B
# Cada módulo corresponde a una fase del pipeline:
#   extract.py        → Descarga de datos crudos desde datos.gob.mx
#   profile.py        → Perfilado y diagnóstico de calidad de datos
#   transform.py      → Limpieza, normalización y estandarización
#   load_dw.py        → Carga al Data Warehouse (esquema estrella en SQLite)
#   build_marts.py    → Construcción de Data Marts analíticos
#   generate_dashboard.py → Generación de dashboard HTML estático
