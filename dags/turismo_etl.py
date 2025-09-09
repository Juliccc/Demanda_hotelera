# dags/mza_turismo_etl.py

from __future__ import annotations
import os
import logging
import requests
import yaml
import pandas as pd
import json
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
    """Versión optimizada que construye specs más rápido."""
    specs = []

    # INDEC
    indec_config = cfg.get("indec", {}).get("eoh", {})
    base_url = indec_config.get("base_url", "")
    months = indec_config.get("months", [])
    
    if base_url and months:
        # Usar list comprehension para mayor eficiencia
        indec_specs = [
            {
                "src": "indec",
                "name": month["name"],
                "url": f"{base_url}{month['name']}",
                "parse": indec_config.get("parse", False),
                "parse_pages": indec_config.get("parse_pages", ""),
                "min_bytes": indec_config.get("min_bytes", DEFAULT_MIN_BYTES),
            }
            for month in months 
            if isinstance(month, dict) and "name" in month
        ]
        specs.extend(indec_specs)
        logger.info(f"INDEC specs: {len(indec_specs)}")

    # YVERA
    yvera_config = cfg.get("yvera", {}).get("oferta_alojamientos", {})
    yvera_url = yvera_config.get("url")
    
    if yvera_url:
        specs.append({
            "src": "yvera",
            "name": Path(yvera_url).name,
            "url": yvera_url,
            "parse": yvera_config.get("parse", False),
            "parse_pages": yvera_config.get("parse_pages", ""),
            "min_bytes": yvera_config.get("min_bytes", DEFAULT_MIN_BYTES),
        })
        logger.info("YVERA spec agregado")

    logger.info(f"Total specs: {len(specs)}")
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
    pool="download_pool",  # Pool específico para descargas
    pool_slots=1,  # Una descarga por slot
    execution_timeout=timedelta(minutes=10),  # Timeout específico
)
def download_file_optimized(
    src: str,
    name: str,
    url: str,
    min_bytes: int,
    base_dirs: Dict[str, str]
) -> Dict[str, Any]:
    """Versión optimizada de descarga."""
    try:
        raw_folder = Path(base_dirs["raw"])
        src_folder = raw_folder / src
        dest_path = src_folder / name
        
        logger.info(f"Descargando {src}/{name}")
        
        # Check rápido si existe
        if dest_path.exists() and dest_path.stat().st_size >= min_bytes:
            size = dest_path.stat().st_size
            logger.info(f"Skip {name}: {size} bytes")
            return {
                "src": src, "name": name, "path": str(dest_path),
                "size": size, "status": "cached", "url": url
            }
        
        # Descarga optimizada
        headers = {
            'User-Agent': 'TurismoBot/1.0',
            'Accept': 'application/pdf,*/*',
            'Connection': 'keep-alive'
        }
        
        # Session reutilizable para mejor rendimiento
        with requests.Session() as session:
            session.headers.update(headers)
            
            with session.get(url, timeout=60, stream=True) as response:
                response.raise_for_status()
                
                total_size = 0
                with open(dest_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=16384):  # Chunks más grandes
                        if chunk:
                            f.write(chunk)
                            total_size += len(chunk)
        
        # Validación rápida
        if total_size < min_bytes:
            dest_path.unlink()
            raise ValueError(f"{name}: {total_size} < {min_bytes} bytes")
        
        logger.info(f"Downloaded {name}: {total_size} bytes")
        
        return {
            "src": src, "name": name, "path": str(dest_path),
            "size": total_size, "status": "downloaded", "url": url
        }
        
    except Exception as e:
        logger.error(f"Download error {name}: {e}")
        raise AirflowException(f"Download failed {name}: {str(e)[:100]}")

@task(
    pool="parse_pool",  # Pool específico para parseo
    execution_timeout=timedelta(minutes=15),
)
def parse_file_optimized(
    file_info: Dict[str, Any],
    parse: bool,
    parse_pages: str,
    base_dirs: Dict[str, str]
) -> Dict[str, Any]:
    """Versión optimizada de parseo."""
    if not parse:
        return {**file_info, "parsed": False, "curated_path": None}
    
    try:
        # Import solo cuando necesario para ahorrar memoria
        import camelot
        
        curated_folder = Path(base_dirs["curated"])
        src_folder = curated_folder / file_info["src"]
        source_path = Path(file_info["path"])
        output_path = src_folder / f"{source_path.stem}.csv"
        
        logger.info(f"Parsing {file_info['name']}")
        
        # Parseo optimizado con parámetros específicos
        tables = camelot.read_pdf(
            str(source_path),
            pages=parse_pages or "1",
            flavor="stream",  # Más rápido para la mayoría de PDFs
            edge_tol=50,  # Tolerancia para detectar bordes
            row_tol=2,    # Tolerancia para filas
        )
        
        if not tables:
            logger.warning(f"No tables in {file_info['name']}")
            return {**file_info, "parsed": False, "curated_path": None}
        
        # Procesamiento eficiente
        dfs = [table.df for table in tables if not table.df.empty]
        
        if not dfs:
            logger.warning(f"Empty tables in {file_info['name']}")
            return {**file_info, "parsed": False, "curated_path": None}
        
        # Concatenar y limpiar en una operación
        combined_df = pd.concat(dfs, ignore_index=True)
        combined_df = combined_df.dropna(how='all').dropna(axis=1, how='all')
        
        # Guardar optimizado
        combined_df.to_csv(output_path, index=False, encoding='utf-8')
        
        logger.info(f"Parsed {file_info['name']}: {len(combined_df)} rows")
        
        return {
            **file_info,
            "parsed": True,
            "curated_path": str(output_path),
            "rows": len(combined_df),
            "columns": len(combined_df.columns)
        }
        
    except Exception as e:
        logger.error(f"Parse error {file_info['name']}: {e}")
        # No fallar completamente por errores de parseo
        return {**file_info, "parsed": False, "parse_error": str(e)[:200]}

@task
def generate_report_optimized(
    processed_files: List[Dict[str, Any]], 
    base_dirs: Dict[str, str]
) -> str:
    """Genera reporte final optimizado."""
    try:
        logs_folder = Path(base_dirs["logs"])
        
        # Estadísticas rápidas
        total_files = len(processed_files)
        successful = sum(1 for f in processed_files if f.get("status") in ["downloaded", "cached"])
        parsed = sum(1 for f in processed_files if f.get("parsed", False))
        total_size = sum(f.get("size", 0) for f in processed_files)
        errors = [f for f in processed_files if f.get("parse_error")]
        
        # Reporte simplificado
        report = {
            "timestamp": datetime.now().isoformat(),
            "summary": {
                "total_files": total_files,
                "successful_downloads": successful,
                "parsed_files": parsed,
                "total_size_mb": round(total_size / (1024*1024), 2),
                "error_count": len(errors)
            },
            "files": [
                {
                    "source": f["src"],
                    "name": f["name"], 
                    "status": f.get("status", "unknown"),
                    "parsed": f.get("parsed", False),
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
            f"{parsed} parsed, {report['summary']['total_size_mb']} MB"
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
    
    # 2. Descarga en paralelo (con límites)
    downloaded_files = download_file_optimized.expand(
        src=[spec["src"] for spec in FILE_SPECS],
        name=[spec["name"] for spec in FILE_SPECS],
        url=[spec["url"] for spec in FILE_SPECS], 
        min_bytes=[spec["min_bytes"] for spec in FILE_SPECS],
        base_dirs=[directories] * len(FILE_SPECS)
    )
    
    # 3. Parseo cuando necesario
    processed_files = parse_file_optimized.expand(
        file_info=downloaded_files,
        parse=[spec["parse"] for spec in FILE_SPECS],
        parse_pages=[spec["parse_pages"] for spec in FILE_SPECS],
        base_dirs=[directories] * len(FILE_SPECS)
    )
    
    # 4. Reporte final
    final_report = generate_report_optimized(processed_files, directories)
    
    # Dependencias simplificadas
    directories >> downloaded_files >> processed_files >> final_report