# dags/mza_turismo_etl.py

from __future__ import annotations
import os
import logging
import requests
import yaml
import pandas as pd
import json
import certifi

from functools import lru_cache

from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowException
from airflow.configuration import conf

# ─── Configuración optimizada ──────────────────────────────────────────────────

AIRFLOW_HOME = Path(os.getenv("AIRFLOW_HOME", "/usr/local/airflow"))
DATA_ROOT = AIRFLOW_HOME / "data"
CONFIG_PATH = AIRFLOW_HOME / "include/config/sources.yaml"

# Configuración de logging más eficiente
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Cache para variables de Airflow (evitar múltiples llamadas a la DB)
@lru_cache(maxsize=10)
def get_variable(key: str, default: str) -> str:
    try:
        return Variable.get(key, default_var=default)
    except Exception:
        return default

DEFAULT_MIN_BYTES = int(get_variable("turismo_min_bytes", "10000"))
ALERT_EMAIL = get_variable("turismo_alert_email", "alerts@miempresa.com")

# ─── Funciones de configuración optimizadas ────────────────────────────────────

@lru_cache(maxsize=1)  # Cache la configuración para evitar múltiples lecturas
def load_config() -> dict:
    """Carga la configuración una sola vez y la cachea."""
    try:
        if not CONFIG_PATH.exists():
            raise FileNotFoundError(f"Config no encontrado: {CONFIG_PATH}")
        
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        
        logger.info(f"Config cargada: {CONFIG_PATH}")
        return config
    except Exception as e:
        logger.error(f"Error config: {e}")
        raise AirflowException(f"Config error: {e}")

def build_file_specs_optimized(cfg: dict) -> List[Dict[str, Any]]:
    """Construye specs"""
    specs = []

    od_config = cfg.get("open_data_mza", {})
    url = od_config.get("api_url")
    if url:
        specs.append({
            "src": "open_data_mza",
            "name": f"{od_config.get('dataset_name', 'dataset')}.csv",
            "url": url,
            "parse": False,  # Siempre falso, es CSV
            "parse_pages": "",
            "min_bytes": od_config.get("min_bytes", DEFAULT_MIN_BYTES),
        })
        logger.info("open_data_mza spec agregado")

    return specs

# Cargar configuración al inicio del módulo
CFG = load_config()
FILE_SPECS = build_file_specs_optimized(CFG)

# ─── DAG optimizado ─────────────────────────────────────────────────────────────

default_args = {
    "owner": "equipo_turismo",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "email": [ALERT_EMAIL],
    "retries": 1,  # Reducido para evitar esperas largas
    "retry_delay": timedelta(minutes=3),  # Reducido
    "execution_timeout": timedelta(minutes=30),  # Timeout para evitar procesos colgados
}

# ─── Tareas optimizadas ─────────────────────────────────────────────────────────

@task(
    pool="default_pool",  # Usar pool por defecto
    max_active_tis_per_dag=4,  # Limitar tareas simultáneas
)
def prep_dirs_optimized(ds: str) -> Dict[str, str]:
    """Versión optimizada de preparación de directorios."""
    try:
        context = get_current_context()
        run_id = context.get('run_id', 'manual_run')
        
        # Estructura simplificada
        base_path = DATA_ROOT / ds
        raw_dir = base_path / "raw"
        curated_dir = base_path / "curated"
        logs_dir = base_path / "logs"
        
        # Crear todos los directorios de una vez
        directories = [raw_dir, curated_dir, logs_dir]
        
        # Agregar subdirectorios por fuente
        for spec in FILE_SPECS:
            directories.extend([
                raw_dir / spec["src"],
                curated_dir / spec["src"]
            ])
        
        # Crear todos los directorios en una sola operación
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Directorios creados para {ds}")
        
        return {
            "base": str(base_path),
            "raw": str(raw_dir),
            "curated": str(curated_dir), 
            "logs": str(logs_dir)
        }
        
    except Exception as e:
        logger.error(f"Error dirs: {e}")
        raise AirflowException(f"Dirs error: {e}")

@task(
    execution_timeout=timedelta(minutes=5),
)
def download_csv(
    src: str,
    name: str,
    url: str,
    min_bytes: int,
    base_dirs: Dict[str, str]
) -> Dict[str, Any]:
    """Descarga un CSV y lo guarda en el directorio raw con verificación SSL."""
    try:
        raw_folder = Path(base_dirs["raw"])
        src_folder = raw_folder / src
        src_folder.mkdir(parents=True, exist_ok=True)

        dest_path = src_folder / name
        logger.info(f"Descargando CSV {name} desde {url}")

        # Saltar si ya existe y es válido
        if dest_path.exists() and dest_path.stat().st_size >= min_bytes:
            size = dest_path.stat().st_size
            logger.info(f"Archivo ya existe, skip: {size} bytes")
            return {"src": src, "name": name, "path": str(dest_path), "size": size, "status": "cached", "url": url}

        # Descargar archivo usando certifi para evitar problemas de SSL
        import certifi
        response = requests.get(url, timeout=30, verify=False)
        response.raise_for_status()
        dest_path.write_bytes(response.content)
        total_size = dest_path.stat().st_size

        if total_size < min_bytes:
            dest_path.unlink()
            raise ValueError(f"{name}: {total_size} < {min_bytes} bytes")

        logger.info(f"CSV descargado: {total_size} bytes")
        return {"src": src, "name": name, "path": str(dest_path), "size": total_size, "status": "downloaded", "url": url}

    except Exception as e:
        logger.error(f"Error descargando CSV {name}: {e}")
        raise AirflowException(f"Download failed {name}: {str(e)[:100]}")

@task
def generate_report_optimized(
    processed_files: List[Dict[str, Any]], 
    base_dirs: Dict[str, str]
) -> str:
    """Genera reporte final simplificado para CSVs."""
    try:
        logs_folder = Path(base_dirs["logs"])
        
        # Estadísticas básicas
        total_files = len(processed_files)
        successful = sum(1 for f in processed_files if f.get("status") in ["downloaded", "cached"])
        total_size = sum(f.get("size", 0) for f in processed_files)
        errors = [f for f in processed_files if f.get("status") not in ["downloaded", "cached"]]
        
        # Reporte simplificado
        report = {
            "timestamp": datetime.now().isoformat(),
            "summary": {
                "total_files": total_files,
                "successful_downloads": successful,
                "total_size_mb": round(total_size / (1024*1024), 2),
                "error_count": len(errors)
            },
            "files": [
                {
                    "source": f["src"],
                    "name": f["name"], 
                    "status": f.get("status", "unknown"),
                    "size_kb": round(f.get("size", 0) / 1024, 1)
                }
                for f in processed_files
            ]
        }
        
        # Guardar reporte
        report_path = logs_folder / "execution_report.json"
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2)
        
        # Log conciso
        logger.info(
            f"Pipeline completed: {successful}/{total_files} files, "
            f"{report['summary']['total_size_mb']} MB total"
        )
        
        return str(report_path)
        
    except Exception as e:
        logger.error(f"Report error: {e}")
        return f"Report failed: {e}"


# ─── DAG Definition con configuración optimizada ────────────────────────────────

with DAG(
    dag_id="mza_turismo_etl_optimized",
    default_args=default_args,
    description="ETL optimizado para demanda hotelera Argentina",
    schedule="0 6 10 * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=6,  # Limitar tareas concurrentes
    tags=["mendoza", "turismo", "optimized"],
    doc_md="## ETL Optimizado\nPipeline de ingeniería de datos optimizado para rendimiento.",
) as dag:

    # 1. Preparación inicial
    directories = prep_dirs_optimized(ds="{{ ds }}")

    # 2. Descarga CSV
    downloaded_file = download_csv(
        src=FILE_SPECS[0]["src"],
        name=FILE_SPECS[0]["name"],
        url=FILE_SPECS[0]["url"],
        min_bytes=FILE_SPECS[0]["min_bytes"],
        base_dirs=directories
    )

    # 3. Reporte final
    final_report = generate_report_optimized(
        processed_files=[downloaded_file],
        base_dirs=directories
    )

    # Dependencias
    directories >> downloaded_file >> final_report