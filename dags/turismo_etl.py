# dags/mza_turismo_etl_final.py

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

# Import condicional para BeautifulSoup
try:
    from bs4 import BeautifulSoup
    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False
    logging.warning("BeautifulSoup4 no disponible - funcionalidad de scraping deshabilitada")

# ─── Configuración ─────────────────────────────────────────────────────────────

AIRFLOW_HOME = Path(os.getenv("AIRFLOW_HOME", "/usr/local/airflow"))
DATA_ROOT = AIRFLOW_HOME / "data"
CONFIG_PATH = AIRFLOW_HOME / "include/config/sources.yaml"

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

@lru_cache(maxsize=10)
def get_variable(key: str, default: str) -> str:
    try:
        return Variable.get(key, default_var=default)
    except Exception:
        return default

ALERT_EMAIL = get_variable("turismo_alert_email", "alerts@proyecto.edu")

# ─── Carga de configuración compatible con tu sources.yaml ─────────────────────

@lru_cache(maxsize=1)
def load_config() -> dict:
    """Carga configuración específica para tu estructura de sources.yaml."""
    try:
        if not CONFIG_PATH.exists():
            logger.error(f"Archivo de configuración no encontrado: {CONFIG_PATH}")
            raise FileNotFoundError(f"Config file missing: {CONFIG_PATH}")
        
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        
        logger.info(f"Configuración cargada exitosamente desde: {CONFIG_PATH}")
        return config
    except Exception as e:
        logger.error(f"Error crítico cargando configuración: {e}")
        raise AirflowException(f"Configuration load failed: {e}")

def build_download_specs(cfg: dict) -> List[Dict[str, Any]]:
    """Construye especificaciones de descarga basadas en tu sources.yaml exacto."""
    specs = []
    defaults = cfg.get("defaults", {})
    
    # 1. open_data_mza - CSV directo
    open_data_config = cfg.get("open_data_mza", {})
    if open_data_config:
        api_url = open_data_config.get("api_url")
        if api_url:
            specs.append({
                "src": "open_data_mza",
                "name": f"{open_data_config.get('dataset_name', 'turismo-data')}.csv",
                "url": api_url,
                "type": "direct_csv",
                "min_bytes": open_data_config.get("min_bytes", 5000),
                "description": "Turismo Internacional - Total País (YVERA)"
            })
            logger.info("✅ Open Data Mendoza spec configurado")
    
    # 2. eti - Dataset page con scraping dinámico
    eti_config = cfg.get("eti", {})
    if eti_config and eti_config.get("dynamic_scraping", False):
        dataset_url = eti_config.get("dataset_url")
        if dataset_url:
            specs.append({
                "src": "eti", 
                "name": "eti_dataset_page",
                "url": dataset_url,
                "type": "dataset_page_scraping",
                "min_bytes": eti_config.get("min_bytes", 10000),
                "description": "Encuesta Turismo Internacional (ETI)"
            })
            logger.info("✅ ETI scraping spec configurado")
    
    logger.info(f"📋 Total especificaciones generadas: {len(specs)}")
    return specs

# Cargar configuración al inicio
try:
    CFG = load_config()
    DOWNLOAD_SPECS = build_download_specs(CFG)
    VALIDATION_CONFIG = CFG.get("validation", {})
    DEFAULTS_CONFIG = CFG.get("defaults", {})
except Exception as e:
    logger.error(f"Error crítico en inicialización: {e}")
    raise

# ─── Configuración del DAG ─────────────────────────────────────────────────────

default_args = {
    "owner": "equipo_turismo",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": DEFAULTS_CONFIG.get("max_retries", 3),
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(seconds=DEFAULTS_CONFIG.get("timeout_seconds", 120) * 5),
}

# ─── Tareas del Pipeline ───────────────────────────────────────────────────────

@task(pool="default_pool")
def create_directories(ds: str) -> Dict[str, str]:
    """Crea estructura de directorios para la fecha de ejecución."""
    try:
        base_path = DATA_ROOT / ds
        
        # Estructura principal
        directories = {
            "base": base_path,
            "raw": base_path / "raw",
            "curated": base_path / "curated", 
            "logs": base_path / "logs",
            "reports": base_path / "reports"
        }
        
        # Subdirectorios por fuente
        for spec in DOWNLOAD_SPECS:
            src = spec["src"]
            directories["raw"] = directories["raw"]
            directories["curated"] = directories["curated"] 
            
            # Crear subdirectorios específicos
            (directories["raw"] / src).mkdir(parents=True, exist_ok=True)
            (directories["curated"] / src).mkdir(parents=True, exist_ok=True)
        
        # Crear todos los directorios principales
        for dir_path in directories.values():
            dir_path.mkdir(parents=True, exist_ok=True)
        
        # Convertir a strings para serialización
        dir_strings = {key: str(path) for key, path in directories.items()}
        
        logger.info(f"✅ Estructura de directorios creada para {ds}")
        return dir_strings
        
    except Exception as e:
        logger.error(f"❌ Error creando directorios: {e}")
        raise AirflowException(f"Directory creation failed: {e}")

@task(execution_timeout=timedelta(minutes=10))
def download_direct_csv(
    spec: Dict[str, Any],
    directories: Dict[str, str]
) -> Dict[str, Any]:
    """Descarga un archivo CSV directamente desde una URL."""
    try:
        src = spec["src"]
        name = spec["name"] 
        url = spec["url"]
        min_bytes = spec["min_bytes"]
        
        raw_dir = Path(directories["raw"]) / src
        dest_path = raw_dir / name
        
        logger.info(f"📥 Descargando {spec['description']}: {name}")
        
        # Verificar si ya existe
        if dest_path.exists() and dest_path.stat().st_size >= min_bytes:
            size = dest_path.stat().st_size
            logger.info(f"✅ Archivo existente válido: {size:,} bytes")
            return {
                "src": src, "name": name, "path": str(dest_path),
                "size": size, "status": "cached", "url": url,
                "description": spec["description"]
            }
        
        # Headers para evitar bloqueos
        headers = {
            'User-Agent': 'Mozilla/5.0 (compatible; TurismoDataPipeline/1.0)',
            'Accept': 'text/csv,application/csv,text/plain,*/*',
            'Accept-Language': 'es-AR,es;q=0.9,en;q=0.8',
        }
        
        # Descarga con timeout y SSL válido
        chunk_size = DEFAULTS_CONFIG.get("chunk_size", 8192)
        timeout = DEFAULTS_CONFIG.get("timeout_seconds", 120)
        
        with requests.Session() as session:
            session.headers.update(headers)
            
            response = session.get(
                url,
                timeout=timeout,
                stream=True,
                verify=False
            )
            response.raise_for_status()
            
            # Escribir archivo por chunks
            total_size = 0
            with open(dest_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        total_size += len(chunk)
        
        # Validar tamaño mínimo
        if total_size < min_bytes:
            dest_path.unlink()
            raise ValueError(f"Archivo muy pequeño: {total_size} < {min_bytes} bytes")
        
        logger.info(f"✅ Descarga exitosa: {name} - {total_size:,} bytes")
        
        return {
            "src": src, "name": name, "path": str(dest_path),
            "size": total_size, "status": "downloaded", "url": url,
            "description": spec["description"]
        }
        
    except Exception as e:
        logger.error(f"❌ Error descargando {spec.get('name', 'unknown')}: {e}")
        return {
            "src": spec.get("src", "unknown"),
            "name": spec.get("name", "unknown"),
            "path": "", "size": 0, "status": "error",
            "url": spec.get("url", ""), "error": str(e)[:200]
        }

@task(execution_timeout=timedelta(minutes=15))
def scrape_and_download_csvs(
    spec: Dict[str, Any],
    directories: Dict[str, str]
) -> List[Dict[str, Any]]:
    """Hace scraping de una página de dataset y descarga los CSVs encontrados."""
    if not BS4_AVAILABLE:
        logger.error("BeautifulSoup4 no disponible - scraping deshabilitado")
        return [{
            "src": spec["src"], "name": "scraping_disabled", 
            "status": "error", "error": "BeautifulSoup4 not available"
        }]
    
    try:
        src = spec["src"]
        dataset_url = spec["url"]
        min_bytes = spec["min_bytes"]
        
        logger.info(f"🔍 Scraping CSV links desde: {dataset_url}")
        
        # Headers anti-bot
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        }
        
        # Obtener HTML de la página
        response = requests.get(
            dataset_url,
            headers=headers,
            timeout=60,
            verify=certifi.where()
        )
        response.raise_for_status()
        
        # Parsear HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Buscar enlaces CSV con múltiples estrategias
        csv_urls = set()
        
        # Estrategia 1: Enlaces que terminan en .csv
        for link in soup.find_all('a', href=True):
            href = link['href']
            if href.endswith('.csv'):
                if href.startswith('http'):
                    csv_urls.add(href)
                elif href.startswith('/'):
                    from urllib.parse import urljoin
                    csv_urls.add(urljoin(dataset_url, href))
        
        # Estrategia 2: Enlaces con texto "CSV" o "csv"
        for link in soup.find_all('a', href=True):
            link_text = link.get_text().lower()
            if 'csv' in link_text:
                href = link['href']
                if '.csv' in href:
                    if href.startswith('http'):
                        csv_urls.add(href)
                    elif href.startswith('/'):
                        from urllib.parse import urljoin
                        csv_urls.add(urljoin(dataset_url, href))
        
        csv_list = list(csv_urls)[:10]  # Limitar a 10 archivos máximo
        
        logger.info(f"📊 Encontrados {len(csv_list)} enlaces CSV para descargar")
        
        if not csv_list:
            logger.warning("⚠️ No se encontraron enlaces CSV en la página")
            return [{
                "src": src, "name": "no_csvs_found",
                "status": "warning", "url": dataset_url,
                "message": "No CSV links found on page"
            }]
        
        # Descargar cada CSV encontrado
        results = []
        raw_dir = Path(directories["raw"]) / src
        
        for i, csv_url in enumerate(csv_list, 1):
            try:
                # Generar nombre de archivo
                csv_name = csv_url.split('/')[-1]
                if not csv_name.endswith('.csv'):
                    csv_name = f"eti_dataset_{i}.csv"
                
                # Limpiar nombre de archivo
                import re
                csv_name = re.sub(r'[^\w\-_.]', '_', csv_name)
                
                dest_path = raw_dir / csv_name
                
                logger.info(f"📥 Descargando CSV {i}/{len(csv_list)}: {csv_name}")
                
                # Descargar archivo
                csv_response = requests.get(
                    csv_url,
                    headers=headers,
                    timeout=90,
                    stream=True,
                    verify=False
                )
                csv_response.raise_for_status()
                
                # Guardar archivo
                total_size = 0
                with open(dest_path, 'wb') as f:
                    for chunk in csv_response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            total_size += len(chunk)
                
                # Validar tamaño (warning, no error)
                status = "downloaded"
                if total_size < min_bytes:
                    logger.warning(f"⚠️ Archivo pequeño {csv_name}: {total_size} bytes")
                    status = "downloaded_small"
                
                results.append({
                    "src": src, "name": csv_name, "path": str(dest_path),
                    "size": total_size, "status": status, "url": csv_url
                })
                
                logger.info(f"✅ {csv_name}: {total_size:,} bytes")
                
            except Exception as e:
                logger.error(f"❌ Error descargando CSV {i}: {e}")
                results.append({
                    "src": src, "name": f"error_csv_{i}",
                    "status": "error", "url": csv_url,
                    "error": str(e)[:150]
                })
        
        successful = len([r for r in results if r["status"].startswith("downloaded")])
        logger.info(f"🎯 Scraping completado: {successful}/{len(csv_list)} CSVs descargados")
        
        return results
        
    except Exception as e:
        logger.error(f"❌ Error en scraping de {spec.get('url', 'unknown')}: {e}")
        return [{
            "src": spec.get("src", "unknown"),
            "name": "scraping_failed",
            "status": "error",
            "url": spec.get("url", ""),
            "error": str(e)[:200]
        }]

@task
def validate_downloaded_data(
    all_downloads: List[Any],
    directories: Dict[str, str]
) -> Dict[str, Any]:
    """Valida la calidad de los datos descargados."""
    try:
        # Aplanar lista de descargas
        files = []
        for download in all_downloads:
            if isinstance(download, list):
                files.extend(download)
            else:
                files.append(download)
        
        validation_results = {
            "timestamp": datetime.now().isoformat(),
            "total_files": len(files),
            "successful_files": 0,
            "failed_files": 0,
            "data_quality_issues": [],
            "file_validations": []
        }
        
        enable_quality_checks = VALIDATION_CONFIG.get("enable_data_quality_checks", True)
        min_rows = VALIDATION_CONFIG.get("min_rows_per_table", 5)
        
        for file_info in files:
            file_validation = {
                "file": file_info.get("name", "unknown"),
                "src": file_info.get("src", "unknown"),
                "status": file_info.get("status", "unknown"),
                "size_mb": round(file_info.get("size", 0) / (1024*1024), 3)
            }
            
            if file_info.get("status", "").startswith("downloaded"):
                validation_results["successful_files"] += 1
                
                # Validación de calidad si está habilitada
                if enable_quality_checks and file_info.get("path"):
                    try:
                        file_path = Path(file_info["path"])
                        if file_path.exists() and file_path.suffix.lower() == '.csv':
                            df = pd.read_csv(file_path, nrows=100)  # Solo primeras 100 filas para validación
                            
                            file_validation.update({
                                "rows_sampled": len(df),
                                "columns": len(df.columns),
                                "has_data": len(df) >= min_rows,
                                "empty_columns": df.isna().all().sum(),
                                "data_quality": "good" if len(df) >= min_rows else "needs_review"
                            })
                            
                            if len(df) < min_rows:
                                validation_results["data_quality_issues"].append(
                                    f"{file_info['name']}: Solo {len(df)} filas (mínimo: {min_rows})"
                                )
                    
                    except Exception as e:
                        file_validation["validation_error"] = str(e)[:100]
            else:
                validation_results["failed_files"] += 1
            
            validation_results["file_validations"].append(file_validation)
        
        # Calcular métricas de éxito
        if validation_results["total_files"] > 0:
            success_rate = validation_results["successful_files"] / validation_results["total_files"] * 100
            validation_results["success_rate"] = round(success_rate, 1)
        else:
            validation_results["success_rate"] = 0.0
        
        # Guardar resultados de validación
        reports_dir = Path(directories["reports"])
        validation_path = reports_dir / f"data_validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(validation_path, 'w', encoding='utf-8') as f:
            json.dump(validation_results, f, indent=2, ensure_ascii=False)
        
        logger.info(f"📊 Validación completada: {validation_results['successful_files']}/{validation_results['total_files']} archivos exitosos ({validation_results['success_rate']}%)")
        
        return validation_results
        
    except Exception as e:
        logger.error(f"❌ Error en validación de datos: {e}")
        return {
            "timestamp": datetime.now().isoformat(),
            "error": str(e),
            "success": False
        }

@task
def generate_pipeline_report(
    validation_results: Dict[str, Any],
    directories: Dict[str, str]
) -> str:
    """Genera el reporte final del pipeline."""
    try:
        # Reporte consolidado
        pipeline_report = {
            "pipeline_execution": {
                "timestamp": datetime.now().isoformat(),
                "dag_id": "mza_turismo_etl_final",
                "execution_date": directories["base"].split('/')[-1],
                "status": "completed"
            },
            "data_acquisition": validation_results,
            "configuration_used": {
                "sources": len(DOWNLOAD_SPECS),
                "validation_enabled": VALIDATION_CONFIG.get("enable_data_quality_checks", True),
                "timeout_seconds": DEFAULTS_CONFIG.get("timeout_seconds", 120)
            },
            "next_steps": [
                "Datos listos para Análisis Exploratorio (Segunda Entrega - 01/10/2025)",
                "Revisar archivos en directorio curated/",
                "Ejecutar notebook de EDA sobre datasets descargados"
            ]
        }
        
        # Guardar reporte final
        reports_dir = Path(directories["reports"])
        report_path = reports_dir / f"pipeline_execution_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(pipeline_report, f, indent=2, ensure_ascii=False)
        
        # Log resumen ejecutivo
        success_rate = validation_results.get("success_rate", 0)
        total_files = validation_results.get("total_files", 0)
        successful_files = validation_results.get("successful_files", 0)
        
        logger.info("=" * 60)
        logger.info("🎯 PIPELINE DE TURISMO COMPLETADO")
        logger.info("=" * 60)
        logger.info(f"📊 Archivos procesados: {successful_files}/{total_files}")
        logger.info(f"✅ Tasa de éxito: {success_rate}%")
        logger.info(f"📁 Directorio de datos: {directories['base']}")
        logger.info(f"📋 Reporte completo: {report_path.name}")
        logger.info("🚀 LISTO PARA SEGUNDA ENTREGA - ANÁLISIS EXPLORATORIO")
        logger.info("=" * 60)
        
        return str(report_path)
        
    except Exception as e:
        logger.error(f"❌ Error generando reporte final: {e}")
        return f"Report generation failed: {e}"

# ─── DAG Definition Final ──────────────────────────────────────────────────────

with DAG(
    dag_id="mza_turismo_etl_final",
    default_args=default_args,
    description="🏨 Pipeline Final ETL - Demanda Hotelera Argentina (Primera Entrega)",
    schedule="@monthly",  # Ejecución mensual
    start_date=datetime(2024, 8, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=8,
    tags=["argentina", "turismo", "primera-entrega", "final", "demo"],
    doc_md="""
    ## 🏨 Pipeline ETL Final - Primera Entrega
    
    **Proyecto**: Predicción de Demanda Hotelera en Argentina
    **Fecha de entrega**: 10/09/2025
    **Estado**: Listo para demostración
    
    ### Fuentes de datos configuradas:
    - 📊 **YVERA**: Turismo Internacional Total País
    - 🔍 **ETI**: Encuesta Turismo Internacional (scraping dinámico)
    
    ### Funcionalidades:
    - ✅ Descarga automática de datos
    - ✅ Validación de calidad
    - ✅ Reportes detallados
    - ✅ Manejo robusto de errores
    
    ### Próximos pasos:
    1. Ejecutar este DAG para obtener datos
    2. Revisar reportes de calidad
    3. Preparar notebook para análisis exploratorio
    """,
) as dag:

    # 1. Preparación del entorno
    dirs = create_directories(ds="{{ ds }}")
    
    # 2. Procesar cada tipo de fuente según configuración
    all_downloads = []
    
    for spec in DOWNLOAD_SPECS:
        if spec["type"] == "direct_csv":
            # Descarga directa de CSV
            download_task = download_direct_csv(spec=spec, directories=dirs)
            all_downloads.append(download_task)
            
        elif spec["type"] == "dataset_page_scraping":
            # Scraping + descarga múltiple
            scraping_task = scrape_and_download_csvs(spec=spec, directories=dirs)
            all_downloads.append(scraping_task)
    
    # 3. Validación de calidad de todos los datos
    data_validation = validate_downloaded_data(
        all_downloads=all_downloads,
        directories=dirs
    )
    
    # 4. Reporte final del pipeline
    final_report = generate_pipeline_report(
        validation_results=data_validation,
        directories=dirs
    )
    
    # Dependencias del pipeline
    dirs >> all_downloads >> data_validation >> final_report