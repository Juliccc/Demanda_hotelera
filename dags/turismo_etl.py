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

AIRFLOW_HOME = Path("/usr/local/airflow")
# Configurar para guardar en volumen montado (accesible desde máquina local)
LOCAL_DATA_ROOT = Path("/opt/airflow/data_local")  # Volumen montado
DATA_ROOT = LOCAL_DATA_ROOT if LOCAL_DATA_ROOT.exists() else AIRFLOW_HOME / "data"
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

# ─── Carga de configuración expandida ──────────────────────────────────────────

@lru_cache(maxsize=1)
def load_config() -> dict:
    """Carga configuración expandida con fallbacks."""
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

def build_enhanced_download_specs(cfg: dict) -> List[Dict[str, Any]]:
    """Construye especificaciones expandidas incluyendo nuevas fuentes."""
    specs = []
    defaults = cfg.get("defaults", {})
    
    # 1. Fuentes turísticas originales
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
                "description": "Turismo Internacional - Total País (YVERA)",
                "category": "turismo"
            })
            logger.info("✅ Open Data Mendoza spec configurado")
    
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
                "description": "Encuesta Turismo Internacional (ETI)",
                "category": "turismo"
            })
            logger.info("✅ ETI scraping spec configurado")
    
    # 2. Variables económicas - BCRA
    bcra_usd_config = cfg.get("bcra_usd", {})
    if bcra_usd_config:
        api_url = bcra_usd_config.get("api_url")
        if api_url:
            specs.append({
                "src": "bcra",
                "name": "cotizacion_usd_historica.json",
                "url": api_url,
                "type": "api_json",
                "min_bytes": bcra_usd_config.get("min_bytes", 1000),
                "description": "Cotización USD oficial BCRA",
                "category": "economico",
                "headers": bcra_usd_config.get("headers", {})
            })
            logger.info("✅ BCRA USD spec configurado")
    
    bcra_inflation_config = cfg.get("bcra_inflation", {})
    if bcra_inflation_config:
        api_url = bcra_inflation_config.get("api_url")
        if api_url:
            specs.append({
                "src": "bcra",
                "name": "inflacion_mensual.json", 
                "url": api_url,
                "type": "api_json",
                "min_bytes": bcra_inflation_config.get("min_bytes", 1000),
                "description": "Inflación mensual BCRA",
                "category": "economico",
                "headers": bcra_inflation_config.get("headers", {})
            })
            logger.info("✅ BCRA Inflación spec configurado")
    
    # 3. Datos INDEC adicionales
    indec_pib_config = cfg.get("indec_pib", {})
    if indec_pib_config:
        api_url = indec_pib_config.get("api_url")
        if api_url:
            specs.append({
                "src": "indec",
                "name": "pib_mensual.json",
                "url": api_url, 
                "type": "api_json",
                "min_bytes": indec_pib_config.get("min_bytes", 2000),
                "description": "PIB mensual Argentina",
                "category": "economico"
            })
            logger.info("✅ INDEC PIB spec configurado")
    
    # 4. Alojamiento específico Mendoza
    alojamiento_config = cfg.get("alojamiento_mendoza", {})
    if alojamiento_config:
        api_url = alojamiento_config.get("api_url")
        if api_url:
            specs.append({
                "src": "alojamiento",
                "name": "establecimientos_mendoza.csv",
                "url": api_url,
                "type": "direct_csv", 
                "min_bytes": alojamiento_config.get("min_bytes", 10000),
                "description": "Establecimientos alojamiento Mendoza",
                "category": "infraestructura",
                "filter_province": alojamiento_config.get("filter_province", "Mendoza")
            })
            logger.info("✅ Alojamiento Mendoza spec configurado")
    
    logger.info(f"📋 Total especificaciones generadas: {len(specs)}")
    return specs

# Cargar configuración al inicio
try:
    CFG = load_config()
    DOWNLOAD_SPECS = build_enhanced_download_specs(CFG)
    VALIDATION_CONFIG = CFG.get("validation", {})
    DEFAULTS_CONFIG = CFG.get("defaults", {})
    AGGREGATION_CONFIG = CFG.get("aggregation", {})
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
    "execution_timeout": timedelta(seconds=DEFAULTS_CONFIG.get("timeout_seconds", 180) * 3),
}

# ─── Tareas expandidas del Pipeline ────────────────────────────────────────────

@task(pool="default_pool")
def create_enhanced_directories(ds: str) -> Dict[str, str]:
    """Crea estructura de directorios expandida."""
    try:
        base_path = DATA_ROOT / ds
        
        directories = {
            "base": base_path,
            "raw": base_path / "raw",
            "curated": base_path / "curated", 
            "processed": base_path / "processed",  # Nuevo
            "aggregated": base_path / "aggregated",  # Nuevo
            "features": base_path / "features",  # Nuevo
            "logs": base_path / "logs",
            "reports": base_path / "reports"
        }
        
        # Subdirectorios por categoría
        categories = ["turismo", "economico", "infraestructura", "climatico"]
        for category in categories:
            for dir_type in ["raw", "curated", "processed"]:
                (directories[dir_type] / category).mkdir(parents=True, exist_ok=True)
        
        # Crear todos los directorios principales
        for dir_path in directories.values():
            dir_path.mkdir(parents=True, exist_ok=True)
        
        dir_strings = {key: str(path) for key, path in directories.items()}
        
        logger.info(f"✅ Estructura expandida creada para {ds}")
        return dir_strings
        
    except Exception as e:
        logger.error(f"❌ Error creando directorios: {e}")
        raise AirflowException(f"Directory creation failed: {e}")

@task(execution_timeout=timedelta(minutes=10))
def download_direct_csv_enhanced(
    spec: Dict[str, Any],
    directories: Dict[str, str]
) -> Dict[str, Any]:
    """Descarga CSV con categorización."""
    try:
        src = spec["src"]
        name = spec["name"] 
        url = spec["url"]
        min_bytes = spec["min_bytes"]
        category = spec.get("category", "general")
        
        raw_dir = Path(directories["raw"]) / category
        dest_path = raw_dir / name
        
        logger.info(f"📥 Descargando {spec['description']}: {name}")
        
        if dest_path.exists() and dest_path.stat().st_size >= min_bytes:
            size = dest_path.stat().st_size
            logger.info(f"✅ Archivo existente válido: {size:,} bytes")
            return {
                "src": src, "name": name, "path": str(dest_path),
                "size": size, "status": "cached", "url": url,
                "description": spec["description"], "category": category
            }
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (compatible; TurismoDataPipeline/2.0)',
            'Accept': 'text/csv,application/csv,text/plain,*/*',
            'Accept-Language': 'es-AR,es;q=0.9,en;q=0.8',
        }
        
        chunk_size = DEFAULTS_CONFIG.get("chunk_size", 8192)
        timeout = DEFAULTS_CONFIG.get("timeout_seconds", 180)
        
        with requests.Session() as session:
            session.headers.update(headers)
            
            response = session.get(url, timeout=timeout, stream=True, verify=False)
            response.raise_for_status()
            
            total_size = 0
            with open(dest_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        total_size += len(chunk)
        
        if total_size < min_bytes:
            dest_path.unlink()
            raise ValueError(f"Archivo muy pequeño: {total_size} < {min_bytes} bytes")
        
        logger.info(f"✅ Descarga exitosa: {name} - {total_size:,} bytes")
        
        return {
            "src": src, "name": name, "path": str(dest_path),
            "size": total_size, "status": "downloaded", "url": url,
            "description": spec["description"], "category": category
        }
        
    except Exception as e:
        logger.error(f"❌ Error descargando {spec.get('name', 'unknown')}: {e}")
        return {
            "src": spec.get("src", "unknown"),
            "name": spec.get("name", "unknown"),
            "path": "", "size": 0, "status": "error",
            "url": spec.get("url", ""), "error": str(e)[:200],
            "category": spec.get("category", "unknown")
        }

@task(execution_timeout=timedelta(minutes=12))
def download_api_json(
    spec: Dict[str, Any],
    directories: Dict[str, str]
) -> Dict[str, Any]:
    """Nueva función para descargar datos JSON de APIs."""
    try:
        src = spec["src"]
        name = spec["name"]
        url = spec["url"]
        category = spec.get("category", "general")
        
        raw_dir = Path(directories["raw"]) / category
        dest_path = raw_dir / name
        
        logger.info(f"🔗 Descargando API {spec['description']}: {name}")
        
        headers = spec.get("headers", {})
        headers.update({
            'User-Agent': 'TurismoDataPipeline/2.0',
            'Accept': 'application/json'
        })
        
        timeout = DEFAULTS_CONFIG.get("timeout_seconds", 180)
        
        response = requests.get(url, headers=headers, timeout=timeout, verify=False)
        response.raise_for_status()
        
        # Validar que sea JSON válido
        try:
            json_data = response.json()
        except json.JSONDecodeError as e:
            raise ValueError(f"Respuesta no es JSON válido: {e}")
        
        # Guardar JSON
        with open(dest_path, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False)
        
        file_size = dest_path.stat().st_size
        
        logger.info(f"✅ API descargada: {name} - {file_size:,} bytes")
        
        return {
            "src": src, "name": name, "path": str(dest_path),
            "size": file_size, "status": "downloaded", "url": url,
            "description": spec["description"], "category": category,
            "data_type": "json", "records_count": len(json_data) if isinstance(json_data, list) else 1
        }
        
    except Exception as e:
        logger.error(f"❌ Error descargando API {spec.get('name', 'unknown')}: {e}")
        return {
            "src": spec.get("src", "unknown"),
            "name": spec.get("name", "unknown"),
            "path": "", "size": 0, "status": "error",
            "url": spec.get("url", ""), "error": str(e)[:200],
            "category": spec.get("category", "unknown")
        }

@task(execution_timeout=timedelta(minutes=15))
def scrape_and_download_csvs_enhanced(
    spec: Dict[str, Any],
    directories: Dict[str, str]
) -> List[Dict[str, Any]]:
    """Scraping mejorado con categorización."""
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
        category = spec.get("category", "general")
        
        logger.info(f"🔍 Scraping CSV links desde: {dataset_url}")
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        }
        
        response = requests.get(dataset_url, headers=headers, timeout=90, verify=False)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        csv_urls = set()
        
        for link in soup.find_all('a', href=True):
            href = link['href']
            if href.endswith('.csv'):
                if href.startswith('http'):
                    csv_urls.add(href)
                elif href.startswith('/'):
                    from urllib.parse import urljoin
                    csv_urls.add(urljoin(dataset_url, href))
        
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
        
        csv_list = list(csv_urls)[:12]  # Aumentado límite
        
        logger.info(f"📊 Encontrados {len(csv_list)} enlaces CSV para descargar")
        
        if not csv_list:
            logger.warning("⚠️ No se encontraron enlaces CSV en la página")
            return [{
                "src": src, "name": "no_csvs_found",
                "status": "warning", "url": dataset_url,
                "message": "No CSV links found on page", "category": category
            }]
        
        results = []
        raw_dir = Path(directories["raw"]) / category
        
        for i, csv_url in enumerate(csv_list, 1):
            try:
                csv_name = csv_url.split('/')[-1]
                if not csv_name.endswith('.csv'):
                    csv_name = f"eti_dataset_{i}.csv"
                
                import re
                csv_name = re.sub(r'[^\w\-_.]', '_', csv_name)
                
                dest_path = raw_dir / csv_name
                
                logger.info(f"📥 Descargando CSV {i}/{len(csv_list)}: {csv_name}")
                
                csv_response = requests.get(csv_url, headers=headers, timeout=120, stream=True, verify=False)
                csv_response.raise_for_status()
                
                total_size = 0
                with open(dest_path, 'wb') as f:
                    for chunk in csv_response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            total_size += len(chunk)
                
                status = "downloaded"
                if total_size < min_bytes:
                    logger.warning(f"⚠️ Archivo pequeño {csv_name}: {total_size} bytes")
                    status = "downloaded_small"
                
                results.append({
                    "src": src, "name": csv_name, "path": str(dest_path),
                    "size": total_size, "status": status, "url": csv_url,
                    "category": category
                })
                
                logger.info(f"✅ {csv_name}: {total_size:,} bytes")
                
            except Exception as e:
                logger.error(f"❌ Error descargando CSV {i}: {e}")
                results.append({
                    "src": src, "name": f"error_csv_{i}",
                    "status": "error", "url": csv_url,
                    "error": str(e)[:150], "category": category
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
            "error": str(e)[:200],
            "category": spec.get("category", "unknown")
        }]

@task
def process_and_standardize_data(
    all_downloads: List[Any],
    directories: Dict[str, str]
) -> Dict[str, Any]:
    """Procesa y estandariza todos los datos descargados con validaciones robustas."""
    try:
        files = []
        for download in all_downloads:
            if isinstance(download, list):
                files.extend(download)
            else:
                files.append(download)
        
        processed_files = {
            "turismo": [],
            "economico": [], 
            "infraestructura": [],
            "general": []
        }
        
        processed_dir = Path(directories["processed"])
        
        for file_info in files:
            if not file_info.get("status", "").startswith("downloaded"):
                continue
            
            path = file_info.get("path", "")
            category = file_info.get("category", "general")
            
            try:
                if path.endswith(".csv"):
                    df = pd.read_csv(path, encoding='utf-8')
                elif path.endswith(".json"):
                    with open(path, 'r', encoding='utf-8') as f:
                        json_data = json.load(f)
                    
                    # Validación para arrays de igual longitud
                    if file_info.get("src") == "bcra":
                        # BCRA API formato: {"results": [{"index":..., "value":...}, ...]}
                        if isinstance(json_data, dict) and "results" in json_data:
                            results = json_data["results"]
                            dates = [item.get("index") for item in results if "index" in item]
                            values = [item.get("value") for item in results if "value" in item]
                            if len(dates) != len(values):
                                logger.error(f"Arrays de diferente longitud en {file_info['name']}")
                                continue
                            df = pd.DataFrame({"fecha": dates, "valor": values})
                        else:
                            df = pd.DataFrame(json_data)
                    elif file_info.get("src") == "indec":
                        # INDEC API formato: {"data": [{...}]}
                        if isinstance(json_data, dict) and "data" in json_data:
                            df = pd.DataFrame(json_data["data"])
                        else:
                            df = pd.DataFrame(json_data)
                    else:
                        df = pd.DataFrame(json_data)
                else:
                    logger.warning(f"Tipo de archivo no soportado: {path}")
                    continue
                
                # Estandarizar columnas de fecha
                date_columns = ['fecha', 'date', 'periodo', 'period', 'index']
                for col in df.columns:
                    if isinstance(col, str) and col.lower() in date_columns:
                        try:
                            df[col] = pd.to_datetime(df[col])
                            df = df.rename(columns={col: 'fecha_std'})
                            break
                        except Exception as e:
                            logger.warning(f"Error convirtiendo columna {col} a fecha: {e}")
                            continue

                # Si las columnas son 0,1, renómbralas manualmente
                if list(df.columns) == [0, 1]:
                    df.columns = ['fecha_std', 'valor']
            
                # Filtrar datos de Mendoza si es relevante
                if category in ["turismo", "infraestructura"]:
                    mendoza_keywords = ["mendoza", "mza", "cuyo", "50"]  # Código INDEC Mendoza
                    mendoza_mask = pd.Series([False] * len(df))
                    
                    for col in df.columns:
                        if isinstance(col, str) and df[col].dtype == object:
                            mendoza_mask = mendoza_mask | df[col].astype(str).str.contains(
                                "|".join(mendoza_keywords), case=False, na=False
                            )
                    
                    if mendoza_mask.any():
                        df = df[mendoza_mask]
                        logger.info(f"Filtrado Mendoza aplicado a {file_info['name']}: {len(df)} registros")
                
                # Guardar archivo procesado
                output_path = processed_dir / category / f"processed_{file_info['name'].replace('.json', '.csv')}"
                df.to_csv(output_path, index=False, encoding='utf-8')
                
                processed_files[category].append({
                    "original_file": file_info["name"],
                    "processed_path": str(output_path),
                    "rows": len(df),
                    "columns": len(df.columns),
                    "has_date_column": 'fecha_std' in df.columns,
                    "date_range": f"{df['fecha_std'].min()} - {df['fecha_std'].max()}" if 'fecha_std' in df.columns else "N/A"
                })
                
                logger.info(f"✅ Procesado {category}/{file_info['name']}: {len(df)} filas")
                
            except Exception as e:
                logger.error(f"❌ Error procesando {file_info['name']}: {e}")
                continue
        
        # Log de archivos con fecha_std
        for category, files in processed_files.items():
            for file_info in files:
                df = pd.read_csv(file_info["processed_path"])
                logger.info(f"{file_info['original_file']} - filas: {len(df)}, tiene fecha_std: {'fecha_std' in df.columns}")
        
        # Resumen de procesamiento
        summary = {
            "timestamp": datetime.now().isoformat(),
            "files_by_category": {cat: len(files) for cat, files in processed_files.items()},
            "total_processed": sum(len(files) for files in processed_files.values()),
            "processed_files": processed_files
        }
        
        # Guardar resumen
        summary_path = processed_dir / "processing_summary.json"
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        
        logger.info(f"📊 Procesamiento completado: {summary['total_processed']} archivos")
        
        return summary
        
    except Exception as e:
        logger.error(f"❌ Error en procesamiento de datos: {e}")
        return {"error": str(e), "success": False}

@task
def create_monthly_features_matrix(
    processing_summary: Dict[str, Any],
    directories: Dict[str, str]
) -> str:
    """Crea matriz de features mensuales para el modelo predictivo."""
    try:
        if not processing_summary.get("success", True):
            logger.error("No se puede crear matriz de features sin datos procesados")
            return ""
        
        processed_dir = Path(directories["processed"])
        features_dir = Path(directories["features"])
        
        # Diccionario para almacenar series temporales por categoría
        time_series_data = {}
        
        # Procesar cada categoría de datos
        for category, files in processing_summary.get("processed_files", {}).items():
            category_data = []
            
            for file_info in files:
                try:
                    df = pd.read_csv(file_info["processed_path"])
                    
                    if 'fecha_std' in df.columns and not df.empty:
                        df['fecha_std'] = pd.to_datetime(df['fecha_std'])
                        
                        # Agregar a nivel mensual
                        df['anio_mes'] = df['fecha_std'].dt.to_period('M')
                        
                        # Identificar columnas numéricas para agregar
                        numeric_cols = df.select_dtypes(include=['number']).columns
                        
                        if len(numeric_cols) > 0:
                            monthly_agg = df.groupby('anio_mes')[numeric_cols].agg(['mean', 'sum', 'count']).reset_index()
                            monthly_agg.columns = ['anio_mes'] + [f"{col[0]}_{col[1]}" for col in monthly_agg.columns[1:]]
                            monthly_agg['fuente'] = file_info["original_file"]
                            category_data.append(monthly_agg)
                            
                except Exception as e:
                    logger.warning(f"Error agregando {file_info['processed_path']}: {e}")
                    continue
            
            if category_data:
                # Concatenar todos los datos de la categoría
                category_df = pd.concat(category_data, ignore_index=True)
                time_series_data[category] = category_df
        
        if not time_series_data:
            logger.error("No se generaron datos de series temporales")
            return ""
        
        # Crear matriz unificada
        base_periods = None
        unified_features = None
        
        for category, df in time_series_data.items():
            if df.empty:
                continue
                
            # Crear rango de períodos base
            if base_periods is None:
                min_period = df['anio_mes'].min()
                max_period = df['anio_mes'].max()
                base_periods = pd.period_range(start=min_period, end=max_period, freq='M')
                unified_features = pd.DataFrame({'anio_mes': base_periods})
            
            # Agregar features de esta categoría
            category_features = df.groupby('anio_mes').agg({
                col: 'mean' for col in df.columns if col not in ['anio_mes', 'fuente']
            }).reset_index()
            
            # Renombrar columnas con prefijo de categoría
            category_features.columns = ['anio_mes'] + [f"{category}_{col}" for col in category_features.columns[1:]]
            
            # Merge con matriz unificada
            unified_features = unified_features.merge(category_features, on='anio_mes', how='left')
        
        # Agregar variables derivadas
        unified_features['anio'] = unified_features['anio_mes'].dt.year
        unified_features['mes'] = unified_features['anio_mes'].dt.month
        unified_features['trimestre'] = unified_features['anio_mes'].dt.quarter
        
        # Variables estacionales
        unified_features['es_verano'] = unified_features['mes'].isin([12, 1, 2]).astype(int)
        unified_features['es_otono'] = unified_features['mes'].isin([3, 4, 5]).astype(int)
        unified_features['es_invierno'] = unified_features['mes'].isin([6, 7, 8]).astype(int)
        unified_features['es_primavera'] = unified_features['mes'].isin([9, 10, 11]).astype(int)
        
        # Variables de eventos importantes
        unified_features['mes_vendimia'] = (unified_features['mes'] == 3).astype(int)
        unified_features['vacaciones_verano'] = unified_features['mes'].isin([12, 1, 2]).astype(int)
        unified_features['vacaciones_invierno'] = (unified_features['mes'] == 7).astype(int)
        
        # Convertir anio_mes a string para compatibilidad
        unified_features['anio_mes_str'] = unified_features['anio_mes'].astype(str)
        
        # Guardar matriz de features
        features_path = features_dir / "monthly_features_matrix.csv"
        unified_features.to_csv(features_path, index=False, encoding='utf-8')
        
        # Guardar metadata de features
        features_metadata = {
            "creation_timestamp": datetime.now().isoformat(),
            "total_months": len(unified_features),
            "date_range": f"{unified_features['anio_mes'].min()} - {unified_features['anio_mes'].max()}",
            "total_features": len(unified_features.columns),
            "feature_categories": {
                "turismo": len([col for col in unified_features.columns if col.startswith('turismo_')]),
                "economico": len([col for col in unified_features.columns if col.startswith('economico_')]),
                "infraestructura": len([col for col in unified_features.columns if col.startswith('infraestructura_')]),
                "temporal": len([col for col in unified_features.columns if col in ['anio', 'mes', 'trimestre']]),
                "estacional": len([col for col in unified_features.columns if col.startswith('es_')]),
                "eventos": len([col for col in unified_features.columns if 'vendimia' in col or 'vacaciones' in col])
            },
            "missing_data_percentage": round((unified_features.isnull().sum().sum() / (len(unified_features) * len(unified_features.columns))) * 100, 2),
            "recommended_target_variables": [
                "turismo_valor_mean",  # Para regresión
                "infraestructura_ocupacion_mean"  # Si está disponible
            ]
        }
        
        metadata_path = features_dir / "features_metadata.json"
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(features_metadata, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Matriz de features creada: {len(unified_features)} meses x {len(unified_features.columns)} variables")
        logger.info(f"Rango temporal: {features_metadata['date_range']}")
        logger.info(f"Datos faltantes: {features_metadata['missing_data_percentage']}%")
        
        return str(features_path)
        
    except Exception as e:
        logger.error(f"Error creando matriz de features: {e}")
        return ""

@task
def validate_enhanced_data(
    all_downloads: List[Any],
    processing_summary: Dict[str, Any],
    directories: Dict[str, str]
) -> Dict[str, Any]:
    """Validación mejorada incluyendo nuevas fuentes."""
    try:
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
            "file_validations": [],
            "category_summary": {
                "turismo": {"files": 0, "status": "unknown"},
                "economico": {"files": 0, "status": "unknown"}, 
                "infraestructura": {"files": 0, "status": "unknown"},
                "general": {"files": 0, "status": "unknown"}
            }
        }
        
        enable_quality_checks = VALIDATION_CONFIG.get("enable_data_quality_checks", True)
        min_rows = VALIDATION_CONFIG.get("min_rows_per_table", 5)
        
        for file_info in files:
            category = file_info.get("category", "general")
            validation_results["category_summary"][category]["files"] += 1
            
            file_validation = {
                "file": file_info.get("name", "unknown"),
                "src": file_info.get("src", "unknown"),
                "category": category,
                "status": file_info.get("status", "unknown"),
                "size_mb": round(file_info.get("size", 0) / (1024*1024), 3)
            }
            
            if file_info.get("status", "").startswith("downloaded"):
                validation_results["successful_files"] += 1
                validation_results["category_summary"][category]["status"] = "ok"
                
                if enable_quality_checks and file_info.get("path"):
                    try:
                        file_path = Path(file_info["path"])
                        if file_path.exists():
                            if file_path.suffix.lower() == '.csv':
                                df = pd.read_csv(file_path, nrows=100)
                            elif file_path.suffix.lower() == '.json':
                                with open(file_path, 'r') as f:
                                    json_data = json.load(f)
                                if isinstance(json_data, list):
                                    df = pd.DataFrame(json_data[:100])
                                else:
                                    df = pd.DataFrame([json_data])
                            else:
                                continue
                            
                            file_validation.update({
                                "rows_sampled": int(len(df)),
                                "columns": int(len(df.columns)),
                                "has_data": len(df) >= min_rows,
                                "empty_columns": int(df.isna().all().sum()),
                                "data_quality": "good" if len(df) >= min_rows else "needs_review"
                            })
                            
                            # Validaciones específicas por categoría
                            if category == "economico":
                                numeric_cols = df.select_dtypes(include=['number']).columns
                                file_validation["numeric_columns"] = len(numeric_cols)
                                if len(numeric_cols) == 0:
                                    validation_results["data_quality_issues"].append(
                                        f"{file_info['name']}: Datos económicos sin columnas numéricas"
                                    )
                            
                            elif category == "turismo":
                                # Buscar columnas relevantes para turismo
                                tourism_keywords = ['turista', 'visitante', 'pernoctacion', 'ocupacion']
                                relevant_cols = [col for col in df.columns if any(keyword in col.lower() for keyword in tourism_keywords)]
                                file_validation["tourism_relevant_columns"] = len(relevant_cols)
                                
                            if len(df) < min_rows:
                                validation_results["data_quality_issues"].append(
                                    f"{file_info['name']}: Solo {len(df)} filas (mínimo: {min_rows})"
                                )
                    
                    except Exception as e:
                        file_validation["validation_error"] = str(e)[:100]
            else:
                validation_results["failed_files"] += 1
                if validation_results["category_summary"][category]["status"] != "ok":
                    validation_results["category_summary"][category]["status"] = "failed"
            
            validation_results["file_validations"].append(file_validation)
        
        # Calcular métricas de éxito
        if validation_results["total_files"] > 0:
            success_rate = validation_results["successful_files"] / validation_results["total_files"] * 100
            validation_results["success_rate"] = round(success_rate, 1)
        else:
            validation_results["success_rate"] = 0.0
        
        # Validar completitud por categoría
        critical_categories = ["turismo", "economico"]
        missing_critical = []
        for category in critical_categories:
            if validation_results["category_summary"][category]["files"] == 0:
                missing_critical.append(category)
        
        if missing_critical:
            validation_results["data_quality_issues"].extend([
                f"Categoría crítica faltante: {cat}" for cat in missing_critical
            ])
        
        # Evaluar readiness para modelo predictivo
        validation_results["model_readiness"] = {
            "has_tourism_data": validation_results["category_summary"]["turismo"]["files"] > 0,
            "has_economic_data": validation_results["category_summary"]["economico"]["files"] > 0,
            "overall_ready": (
                validation_results["success_rate"] >= 70 and
                len(missing_critical) == 0 and
                len(validation_results["data_quality_issues"]) < 5
            )
        }
        
        # Guardar validación
        reports_dir = Path(directories["reports"])
        validation_path = reports_dir / f"enhanced_validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(validation_path, 'w', encoding='utf-8') as f:
            json.dump(validation_results, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Validación completada: {validation_results['successful_files']}/{validation_results['total_files']} archivos exitosos")
        logger.info(f"Listo para modelo: {'SI' if validation_results['model_readiness']['overall_ready'] else 'NO'}")
        
        return validation_results
        
    except Exception as e:
        logger.error(f"Error en validación de datos: {e}")
        return {
            "timestamp": datetime.now().isoformat(),
            "error": str(e),
            "success": False
        }

@task
def generate_enhanced_pipeline_report(
    validation_results: Dict[str, Any],
    processing_summary: Dict[str, Any],
    features_path: str,
    directories: Dict[str, str]
) -> str:
    """Genera reporte final mejorado."""
    try:
        pipeline_report = {
            "pipeline_execution": {
                "timestamp": datetime.now().isoformat(),
                "dag_id": "mza_turismo_etl_enhanced",
                "execution_date": directories["base"].split('/')[-1],
                "status": "completed",
                "version": "2.0"
            },
            "data_acquisition": validation_results,
            "data_processing": processing_summary,
            "feature_engineering": {
                "features_matrix_created": bool(features_path),
                "features_path": features_path,
                "ready_for_modeling": validation_results.get("model_readiness", {}).get("overall_ready", False)
            },
            "configuration_used": {
                "sources_configured": len(DOWNLOAD_SPECS),
                "validation_enabled": VALIDATION_CONFIG.get("enable_data_quality_checks", True),
                "timeout_seconds": DEFAULTS_CONFIG.get("timeout_seconds", 180),
                "aggregation_frequency": AGGREGATION_CONFIG.get("target_frequency", "monthly")
            },
            "data_summary": {
                "categories_processed": list(processing_summary.get("files_by_category", {}).keys()) if processing_summary.get("success", True) else [],
                "total_processed_files": processing_summary.get("total_processed", 0) if processing_summary.get("success", True) else 0,
                "quality_issues": len(validation_results.get("data_quality_issues", [])),
                "critical_data_available": {
                    "tourism": validation_results.get("model_readiness", {}).get("has_tourism_data", False),
                    "economic": validation_results.get("model_readiness", {}).get("has_economic_data", False)
                }
            },
            "next_steps": [
                "Dataset multi-dimensional listo para análisis exploratorio avanzado",
                "Matriz de features mensuales disponible para modelado",
                "Variables económicas y estacionales incorporadas",
                "Preparar notebook para EDA con correlaciones entre variables",
                "Implementar modelos de serie temporal (ARIMA, Prophet, LSTM)"
            ],
            "recommendations": []
        }
        
        # Generar recomendaciones basadas en resultados
        if not pipeline_report["feature_engineering"]["ready_for_modeling"]:
            pipeline_report["recommendations"].append("Revisar calidad de datos antes del modelado")
        
        if validation_results.get("success_rate", 0) < 80:
            pipeline_report["recommendations"].append("Mejorar robustez de descarga de datos")
        
        if len(validation_results.get("data_quality_issues", [])) > 3:
            pipeline_report["recommendations"].append("Implementar validaciones más estrictas")
        
        if not validation_results.get("model_readiness", {}).get("has_economic_data", False):
            pipeline_report["recommendations"].append("Priorizar incorporación de variables económicas")
        
        # Guardar reporte final
        reports_dir = Path(directories["reports"])
        report_path = reports_dir / f"enhanced_pipeline_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(pipeline_report, f, indent=2, ensure_ascii=False)
        
        # Log resumen ejecutivo mejorado
        success_rate = validation_results.get("success_rate", 0)
        total_files = validation_results.get("total_files", 0)
        successful_files = validation_results.get("successful_files", 0)
        ready_for_model = pipeline_report["feature_engineering"]["ready_for_modeling"]
        
        logger.info("=" * 70)
        logger.info("PIPELINE MEJORADO DE DEMANDA HOTELERA COMPLETADO")
        logger.info("=" * 70)
        logger.info(f"Archivos procesados: {successful_files}/{total_files} ({success_rate}%)")
        logger.info(f"Categorías de datos: {', '.join(pipeline_report['data_summary']['categories_processed'])}")
        logger.info(f"Features creados: {'SI' if features_path else 'NO'}")
        logger.info(f"Listo para modelado: {'SI' if ready_for_model else 'NO'}")
        logger.info(f"Variables económicas: {'SI' if pipeline_report['data_summary']['critical_data_available']['economic'] else 'NO'}")
        logger.info(f"Directorio de datos: {directories['base']}")
        logger.info(f"Reporte completo: {report_path.name}")
        
        if ready_for_model:
            logger.info("LISTO PARA MODELADO PREDICTIVO AVANZADO")
        else:
            logger.info("REVISAR CALIDAD DE DATOS ANTES DE MODELADO")
            
        logger.info("=" * 70)
        
        return str(report_path)
        
    except Exception as e:
        logger.error(f"Error generando reporte final: {e}")
        return f"Report generation failed: {e}"

@task
def resolve_dynamic_urls(spec: Dict[str, Any]) -> Dict[str, Any]:
    """Resuelve URLs dinámicas que cambian mensualmente."""
    try:
        if spec.get("type") != "dynamic_url":
            return spec
        
        base_url = spec.get("base_url", "")
        search_pattern = spec.get("search_pattern", "")
        
        logger.info(f"🔍 Resolviendo URL dinámica para {spec['src']}")
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        # Estrategia 1: Buscar en página principal
        response = requests.get(base_url, headers=headers, timeout=60)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Buscar enlaces que coincidan con el patrón
        import re
        pattern = re.compile(search_pattern)
        
        found_urls = []
        for link in soup.find_all('a', href=True):
            href = link['href']
            if pattern.search(href):
                if href.startswith('http'):
                    found_urls.append(href)
                else:
                    from urllib.parse import urljoin
                    found_urls.append(urljoin(base_url, href))
        
        if found_urls:
            # Usar la URL más reciente (última en la lista)
            resolved_url = sorted(found_urls)[-1]
            spec_copy = spec.copy()
            spec_copy["url"] = resolved_url
            spec_copy["type"] = spec.get("fallback_type", "direct_csv")
            logger.info(f"✅ URL resuelta: {resolved_url}")
            return spec_copy
        else:
            logger.warning(f"⚠️ No se encontró URL para patrón: {search_pattern}")
            return spec
            
    except Exception as e:
        logger.error(f"❌ Error resolviendo URL dinámica: {e}")
        return spec

@task
def create_unified_mendoza_dataset(
    all_downloads: List[Any],
    processing_summary: Dict[str, Any],
    directories: Dict[str, str]
) -> str:
    """Crea dataset final unificado de Mendoza y lo guarda localmente."""
    try:
        logger.info("🏗️ Creando dataset unificado de Mendoza...")
        
        # Recopilar todos los archivos procesados
        processed_files = processing_summary.get("processed_files", {})
        all_mendoza_data = []
        
        for category, files in processed_files.items():
            for file_info in files:
                try:
                    df = pd.read_csv(file_info["processed_path"])
                    
                    # Agregar metadatos
                    df['categoria'] = category
                    df['fuente'] = file_info['original_file']
                    df['fecha_procesamiento'] = datetime.now().isoformat()
                    
                    # Solo incluir si tiene datos relevantes
                    if not df.empty and len(df) > 5:
                        all_mendoza_data.append(df)
                        logger.info(f"✅ Incluido {file_info['original_file']}: {len(df)} registros")
                    
                except Exception as e:
                    logger.warning(f"Error leyendo {file_info['processed_path']}: {e}")
                    continue
        
        if not all_mendoza_data:
            logger.error("No se encontraron datos para unificar")
            return ""
        
        # Unificar todos los datasets
        df_unified = pd.concat(all_mendoza_data, ignore_index=True, sort=False)
        
        # Limpiar y estandarizar
        df_unified = df_unified.drop_duplicates()
        
        # Ordenar por fecha si existe
        if 'fecha_std' in df_unified.columns:
            df_unified = df_unified.sort_values('fecha_std')
        
        # Guardar en volumen local montado
        local_data_dir = Path("/opt/airflow/data_local")
        local_data_dir.mkdir(parents=True, exist_ok=True)
        
        output_path = local_data_dir / "mendoza_turismo_dataset_unificado.csv"
        df_unified.to_csv(output_path, index=False, encoding='utf-8')
        
        # También guardar en curated para el pipeline
        curated_path = Path(directories["curated"]) / "mendoza_dataset_final.csv"
        df_unified.to_csv(curated_path, index=False, encoding='utf-8')
        
        # Crear resumen del dataset
        dataset_summary = {
            "creation_timestamp": datetime.now().isoformat(),
            "total_records": len(df_unified),
            "total_columns": len(df_unified.columns),
            "date_range": f"{df_unified['fecha_std'].min()} - {df_unified['fecha_std'].max()}" if 'fecha_std' in df_unified.columns else "N/A",
            "categories_included": df_unified['categoria'].unique().tolist(),
            "sources_included": df_unified['fuente'].unique().tolist(),
            "local_path": str(output_path),
            "pipeline_path": str(curated_path),
            "column_list": df_unified.columns.tolist(),
            "data_types": df_unified.dtypes.astype(str).to_dict(),
            "missing_data_summary": df_unified.isnull().sum().to_dict()
        }
        
        summary_path = local_data_dir / "dataset_summary.json"
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(dataset_summary, f, indent=2, ensure_ascii=False)
        
        logger.info("=" * 60)
        logger.info("📊 DATASET UNIFICADO CREADO EXITOSAMENTE")
        logger.info("=" * 60)
        logger.info(f"Registros totales: {len(df_unified):,}")
        logger.info(f"Columnas: {len(df_unified.columns)}")
        logger.info(f"Categorías: {', '.join(df_unified['categoria'].unique())}")
        logger.info(f"Archivo local: {output_path}")
        logger.info(f"Resumen: {summary_path}")
        logger.info("=" * 60)
        
        return str(output_path)
        
    except Exception as e:
        logger.error(f"❌ Error creando dataset unificado: {e}")
        return ""

# ─── DAG Definition Mejorado ───────────────────────────────────────────────────

with DAG(
    dag_id="mza_turismo_etl_enhanced",
    default_args=default_args,
    description="Pipeline ETL Mejorado - Predicción Demanda Hotelera Mendoza con Variables Económicas",
    schedule="@monthly",
    start_date=datetime(2024, 8, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=10,
    tags=["mendoza", "turismo", "economia", "features", "enhanced", "v2"],
    doc_md="""
    ## Pipeline ETL Mejorado - Demanda Hotelera Mendoza
    
    **Versión 2.0** - Incluye variables económicas, procesamiento avanzado y feature engineering
    
    ### Fuentes de datos expandidas:
    - **Turismo**: YVERA, ETI (datos turísticos oficiales)
    - **Economía**: BCRA (USD, inflación), INDEC (PIB, empleo)
    - **Infraestructura**: Establecimientos hoteleros Mendoza
    - **Temporal**: Variables estacionales y eventos
    
    ### Procesamiento avanzado:
    - Estandarización de fechas y formatos
    - Filtrado geográfico inteligente (Mendoza)
    - Agregación mensual automática
    - Creación de matriz de features para ML
    
    ### Salidas para modelado:
    - Dataset multidimensional procesado
    - Matriz de features mensuales
    - Variables económicas y estacionales
    - Metadata y reportes de calidad
    
    **Objetivo**: Base de datos robusta para modelo predictivo de demanda hotelera
    """,
) as dag:
    # 1. Preparación expandida
    dirs = create_enhanced_directories(ds="{{ ds }}")

    # 2. Resolver URLs dinámicas primero
    resolved_specs = []
    static_specs = []
    for spec in DOWNLOAD_SPECS:
        if spec.get("type") == "dynamic_url":
            resolved_task = resolve_dynamic_urls(spec=spec)
            resolved_specs.append(resolved_task)
        else:
            static_specs.append(spec)

    # 3. Descarga de datos por tipo
    all_downloads = []
    
    # Procesar specs estáticas
    for spec in static_specs:
        tipo = spec.get("type", "")
        if tipo == "direct_csv":
            download_task = download_direct_csv_enhanced(spec=spec, directories=dirs)
            all_downloads.append(download_task)
        elif tipo == "api_json":
            api_task = download_api_json(spec=spec, directories=dirs)
            all_downloads.append(api_task)
        elif tipo == "dataset_page_scraping":
            scraping_task = scrape_and_download_csvs_enhanced(spec=spec, directories=dirs)
            all_downloads.append(scraping_task)
    
    # Procesar specs dinámicas resueltas
    for resolved_spec in resolved_specs:
        # Aquí necesitarías lógica adicional para procesar las specs resueltas
        pass

    # 4. Procesamiento y estandarización
    processing_result = process_and_standardize_data(
        all_downloads=all_downloads,
        directories=dirs
    )

    # 5. Creación de matriz de features
    features_matrix = create_monthly_features_matrix(
        processing_summary=processing_result,
        directories=dirs
    )

    # 6. Crear dataset unificado final (NUEVA TAREA)
    unified_dataset = create_unified_mendoza_dataset(
        all_downloads=all_downloads,
        processing_summary=processing_result,
        directories=dirs
    )

    # 7. Validación mejorada
    enhanced_validation = validate_enhanced_data(
        all_downloads=all_downloads,
        processing_summary=processing_result,
        directories=dirs
    )

    # 8. Reporte final mejorado
    final_enhanced_report = generate_enhanced_pipeline_report(
        validation_results=enhanced_validation,
        processing_summary=processing_result,
        features_path=features_matrix,
        directories=dirs
    )

    # Dependencias del pipeline mejorado
    dirs >> all_downloads >> processing_result >> [features_matrix, unified_dataset] >> enhanced_validation >> final_enhanced_report