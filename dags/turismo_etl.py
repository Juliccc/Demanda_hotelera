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
# Guardar todo en la carpeta 'data/raw' dentro del proyecto
DATA_ROOT = AIRFLOW_HOME / "data" / "raw"
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
    
    # ETI aeropuerto
    eti_aeropuerto_config = cfg.get("eti_aeropuerto", {})
    if eti_aeropuerto_config:
        api_url = eti_aeropuerto_config.get("api_url")
        if api_url:
            specs.append({
                "src": "eti_aeropuerto",
                "name": f"{eti_aeropuerto_config.get('dataset_name', 'eti_mendoza_aeropuerto_turistas_no_residentes')}.csv",
                "url": api_url,
                "type": "direct_csv",
                "min_bytes": eti_aeropuerto_config.get("min_bytes", 2000),
                "description": "ETI Mendoza - Aeropuerto - Turistas no residentes",
                "category": "turismo"
            })
            logger.info("✅ ETI Aeropuerto spec configurado")

    # ETI cristo redentor
    eti_cristo_config = cfg.get("eti_cristo_redentor", {})
    if eti_cristo_config:
        api_url = eti_cristo_config.get("api_url")
        if api_url:
            specs.append({
                "src": "eti_cristo_redentor",
                "name": f"{eti_cristo_config.get('dataset_name', 'eti_mendoza_cristo_redentor_turistas_no_residentes')}.csv",
                "url": api_url,
                "type": "direct_csv",
                "min_bytes": eti_cristo_config.get("min_bytes", 2000),
                "description": "ETI Mendoza - Cristo Redentor - Turistas no residentes",
                "category": "turismo"
            })
            logger.info("✅ ETI Cristo Redentor spec configurado")
    
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
    """Descarga CSV con categorización y validación mejorada."""
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
        
        # Validación mejorada para archivos pequeños
        if total_size < min_bytes:
            # No eliminar automáticamente, sino verificar contenido
            logger.warning(f"⚠️ Archivo pequeño detectado: {name} - {total_size} bytes (mínimo: {min_bytes})")
            
            # Verificar si es un archivo CSV válido con datos
            try:
                df_test = pd.read_csv(dest_path)
                if len(df_test) > 0 and len(df_test.columns) > 2:
                    logger.info(f"✅ Archivo pequeño pero válido: {len(df_test)} filas, {len(df_test.columns)} columnas")
                    return {
                        "src": src, "name": name, "path": str(dest_path),
                        "size": total_size, "status": "downloaded_small_valid", "url": url,
                        "description": spec["description"], "category": category,
                        "validation": f"{len(df_test)} filas válidas"
                    }
                else:
                    logger.error(f"❌ Archivo muy pequeño con pocos datos: {len(df_test)} filas")
                    dest_path.unlink()
                    raise ValueError(f"Archivo con datos insuficientes: {len(df_test)} filas")
            except Exception as e:
                logger.error(f"❌ Error validando archivo pequeño: {e}")
                dest_path.unlink()
                raise ValueError(f"Archivo muy pequeño y no válido: {total_size} < {min_bytes} bytes")
        
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
    """Scraping mejorado ETI - enfocado en turistas no residentes Mendoza."""
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
        
        logger.info(f"🔍 Scraping ETI - Buscando CSVs específicos de Mendoza: {dataset_url}")
        
        # Archivos específicos que necesitamos
        target_files = [
            "turistas_pernoctes_estadia_media_turistas_no_residentes_por_residencia_aeropuerto_mendoza_trimes",
            "turistas_pernoctes_estadia_media_turistas_no_residentes_por_residencia_cristo_redentor_trimestra"
        ]
        
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
        
        # Buscar específicamente los archivos que necesitamos
        target_csv_urls = {}
        
        # Estrategia 1: Buscar enlaces exactos
        for link in soup.find_all('a', href=True):
            href = link['href']
            link_text = link.get_text().strip()
            
            # Verificar si el enlace contiene alguno de nuestros archivos objetivo
            for target_file in target_files:
                if target_file in href.lower() or target_file in link_text.lower():
                    if href.endswith('.csv'):
                        if href.startswith('http'):
                            target_csv_urls[target_file] = href
                        elif href.startswith('/'):
                            from urllib.parse import urljoin
                            target_csv_urls[target_file] = urljoin(dataset_url, href)
                        logger.info(f"✅ Encontrado archivo objetivo: {target_file}")
        
        # Estrategia 2: Buscar por palabras clave si no encontramos los exactos
        if not target_csv_urls:
            logger.info("🔍 Búsqueda exacta fallida, usando palabras clave...")
            keywords = [
                ["turistas", "no_residentes", "mendoza", "aeropuerto"],
                ["turistas", "no_residentes", "cristo", "redentor"],
                ["mendoza", "aeropuerto", "trimestral"],
                ["cristo_redentor", "trimestral"]
            ]
            
            for link in soup.find_all('a', href=True):
                href = link['href']
                link_text = link.get_text().lower()
                
                if href.endswith('.csv'):
                    href_lower = href.lower()
                    
                    # Verificar cada conjunto de palabras clave
                    for i, keyword_set in enumerate(keywords):
                        if all(keyword in href_lower or keyword in link_text for keyword in keyword_set):
                            file_key = f"mendoza_turistas_{i+1}"
                            if href.startswith('http'):
                                target_csv_urls[file_key] = href
                            else:
                                from urllib.parse import urljoin
                                target_csv_urls[file_key] = urljoin(dataset_url, href)
                            logger.info(f"✅ Encontrado por palabras clave: {file_key}")
        
        if not target_csv_urls:
            logger.warning("⚠️ No se encontraron los archivos ETI específicos de Mendoza")
            return [{
                "src": src, "name": "mendoza_csvs_not_found",
                "status": "warning", "url": dataset_url,
                "message": "No se encontraron CSVs específicos de turistas no residentes Mendoza",
                "category": category
            }]
        
        logger.info(f"🎯 Encontrados {len(target_csv_urls)} archivos objetivo de Mendoza")
        
        results = []
        raw_dir = Path(directories["raw"]) / category
        
        for file_key, csv_url in target_csv_urls.items():
            try:
                # Generar nombre descriptivo
                if "aeropuerto" in file_key.lower():
                    csv_name = "eti_mendoza_aeropuerto_turistas_no_residentes.csv"
                elif "cristo" in file_key.lower() or "redentor" in file_key.lower():
                    csv_name = "eti_mendoza_cristo_redentor_turistas_no_residentes.csv"
                else:
                    csv_name = f"eti_mendoza_{file_key}.csv"
                
                dest_path = raw_dir / csv_name
                
                logger.info(f"📥 Descargando ETI Mendoza: {csv_name}")
                logger.info(f"🔗 URL: {csv_url}")
                
                csv_response = requests.get(csv_url, headers=headers, timeout=120, stream=True, verify=False)
                csv_response.raise_for_status()
                
                total_size = 0
                with open(dest_path, 'wb') as f:
                    for chunk in csv_response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            total_size += len(chunk)
                
                # Validar contenido específico
                status = "downloaded"
                try:
                    # Verificar que el CSV tiene las columnas esperadas
                    df_test = pd.read_csv(dest_path, nrows=5)
                    expected_columns = ['fecha', 'periodo', 'residencia', 'turistas', 'no_residentes']
                    has_relevant_columns = any(
                        any(expected in col.lower() for expected in expected_columns)
                        for col in df_test.columns
                    )
                    
                    if not has_relevant_columns:
                        logger.warning(f"⚠️ {csv_name} no tiene columnas esperadas")
                        status = "downloaded_uncertain"
                    else:
                        logger.info(f"✅ {csv_name} validado - contiene columnas relevantes")
                        
                except Exception as e:
                    logger.warning(f"⚠️ No se pudo validar contenido de {csv_name}: {e}")
                
                if total_size < min_bytes:
                    logger.warning(f"⚠️ Archivo pequeño {csv_name}: {total_size} bytes")
                    if status == "downloaded":
                        status = "downloaded_small"
                
                results.append({
                    "src": src, "name": csv_name, "path": str(dest_path),
                    "size": total_size, "status": status, "url": csv_url,
                    "category": category, "file_type": "eti_mendoza_specific",
                    "description": f"Turistas no residentes Mendoza - {file_key}"
                })
                
                logger.info(f"✅ {csv_name}: {total_size:,} bytes - {status}")
                
            except Exception as e:
                logger.error(f"❌ Error descargando {file_key}: {e}")
                results.append({
                    "src": src, "name": f"error_{file_key}",
                    "status": "error", "url": csv_url,
                    "error": str(e)[:150], "category": category
                })
        
        successful = len([r for r in results if r["status"].startswith("downloaded")])
        logger.info(f"🎯 ETI Mendoza completado: {successful}/{len(target_csv_urls)} archivos específicos descargados")
        
        return results
        
    except Exception as e:
        logger.error(f"❌ Error en scraping ETI Mendoza: {e}")
        return [{
            "src": spec.get("src", "unknown"),
            "name": "eti_mendoza_scraping_failed",
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
        
        # DEBUG: Mostrar todos los archivos descargados
        logger.info(f"📥 Total archivos descargados: {len(files)}")
        for file_info in files:
            logger.info(f"  📄 {file_info.get('name', 'unknown')}: {file_info.get('status', 'unknown')} - {file_info.get('category', 'unknown')}")
        
        processed_files = {
            "turismo": [],
            "economico": [], 
            "infraestructura": [],
            "general": []
        }
        
        processed_dir = Path(directories["processed"])
        
        for file_info in files:
            # Incluir archivos pequeños pero válidos - MEJORAR LA CONDICIÓN
            status = file_info.get("status", "")
            if not (status.startswith("downloaded") or status == "cached"):
                logger.warning(f"⚠️ Saltando archivo con status: {status} - {file_info.get('name', 'unknown')}")
                continue
            
            path = file_info.get("path", "")
            category = file_info.get("category", "general")
            
            logger.info(f"🔄 Procesando: {file_info.get('name', 'unknown')} - Categoría: {category}")
            
            try:
                if path.endswith(".csv"):
                    df = pd.read_csv(path, encoding='utf-8')
                    
                    # Log adicional para archivos ETI pequeños
                    if "eti" in file_info.get("name", "").lower():
                        logger.info(f"📊 Procesando ETI {file_info['name']}: {len(df)} filas, columnas: {list(df.columns)}")
                        logger.info(f"📊 Primeras 5 filas del archivo original:\n{df.head()}")
                        
                        # PROCESAR ETI CORRECTAMENTE - Buscar columna de fecha
                        fecha_col = None
                        for col in df.columns:
                            if col.lower() in ['fecha', 'periodo', 'period', 'anio_trimestre', 'trimestre']:
                                fecha_col = col
                                break
                        
                        if fecha_col:
                            logger.info(f"✅ ETI {file_info['name']} - Columna de fecha encontrada: {fecha_col}")
                            logger.info(f"📅 Valores únicos en {fecha_col}: {df[fecha_col].unique()}")
                            
                            # Convertir a datetime y crear indice_tiempo
                            try:
                                df[fecha_col] = pd.to_datetime(df[fecha_col], errors='coerce')
                                df['indice_tiempo'] = df[fecha_col]
                                df['fecha_std'] = df[fecha_col]  # IMPORTANTE: Crear fecha_std
                                logger.info(f"✅ ETI {file_info['name']} - fecha_std creada exitosamente")
                            except Exception as e:
                                logger.error(f"❌ ETI {file_info['name']} - Error creando fecha_std: {e}")
                        else:
                            logger.warning(f"⚠️ ETI {file_info['name']} - No se encontró columna de fecha")
                            # Si no hay columna de fecha, intentar usar indice_tiempo si existe
                            if 'indice_tiempo' in df.columns:
                                try:
                                    df['fecha_std'] = pd.to_datetime(df['indice_tiempo'], errors='coerce')
                                    logger.info(f"✅ ETI {file_info['name']} - fecha_std creada desde indice_tiempo")
                                except Exception as e:
                                    logger.error(f"❌ Error usando indice_tiempo como fecha_std: {e}")
                            # Si no hay indice_tiempo, intentar usar fecha_std si ya existe
                            elif 'fecha_std' in df.columns:
                                try:
                                    df['fecha_std'] = pd.to_datetime(df['fecha_std'], errors='coerce')
                                    logger.info(f"✅ ETI {file_info['name']} - fecha_std ya estaba presente")
                                except Exception as e:
                                    logger.error(f"❌ Error convirtiendo fecha_std existente: {e}")
                        
                elif path.endswith(".json"):
                    with open(path, 'r', encoding='utf-8') as f:
                        json_data = json.load(f)
                    
                    # Validación mejorada para diferentes formatos de API
                    if file_info.get("src") == "bcra":
                        logger.info(f"🏦 Procesando datos BCRA: {file_info['name']}")
                        logger.info(f"🔍 Estructura JSON BCRA: {list(json_data.keys()) if isinstance(json_data, dict) else type(json_data)}")
                        
                        # BCRA API formato: {"results": [{"index":..., "value":...}, ...]}
                        if isinstance(json_data, dict) and "results" in json_data:
                            results = json_data["results"]
                            logger.info(f"📊 BCRA raw results: {len(results)} elementos")
                            
                            # Validación robusta: filtrar elementos válidos
                            valid_items = []
                            for i, item in enumerate(results):
                                if isinstance(item, dict) and "index" in item and "value" in item:
                                    # Verificar que los valores no sean None/null
                                    if item["index"] is not None and item["value"] is not None:
                                        # Verificar que value sea numérico
                                        try:
                                            float(item["value"])
                                            valid_items.append(item)
                                        except (ValueError, TypeError):
                                            logger.warning(f"BCRA item {i}: valor no numérico: {item.get('value')}")
                                    else:
                                        logger.warning(f"BCRA item {i}: index o value es None")
                                else:
                                    logger.warning(f"BCRA item {i}: formato inválido: {item}")
                            
                            if len(valid_items) == 0:
                                logger.error(f"No se encontraron elementos válidos en {file_info['name']}")
                                continue
                            
                            logger.info(f"✅ BCRA elementos válidos: {len(valid_items)}/{len(results)}")
                            
                            # Crear DataFrame solo con elementos válidos
                            try:
                                dates = [item["index"] for item in valid_items]
                                values = [float(item["value"]) for item in valid_items]
                                
                                # Verificación adicional de longitudes
                                if len(dates) != len(values):
                                    logger.error(f"Longitudes diferentes: dates={len(dates)}, values={len(values)}")
                                    # Tomar el mínimo para evitar error
                                    min_len = min(len(dates), len(values))
                                    dates = dates[:min_len]
                                    values = values[:min_len]
                                    logger.warning(f"Ajustado a longitud mínima: {min_len}")
                                
                                df = pd.DataFrame({"fecha": dates, "valor": values})
                                
                                # IMPORTANTE: Crear fecha_std para BCRA
                                df['fecha_std'] = pd.to_datetime(df['fecha'], errors='coerce')
                                logger.info(f"✅ BCRA DataFrame creado: {len(df)} registros válidos con fecha_std")
                                
                            except Exception as e:
                                logger.error(f"Error creando DataFrame BCRA: {e}")
                                continue
                        else:
                            # PROCESAR FORMATO NO ESTÁNDAR DE BCRA
                            logger.warning(f"BCRA formato no estándar en {file_info['name']}")
                            logger.info(f"🔍 Contenido completo del JSON: {json_data}")
                            
                            # Intentar extraer datos del formato actual
                            if isinstance(json_data, dict):
                                # Si es dict con 'data', extraer eso
                                if 'data' in json_data and isinstance(json_data['data'], list):
                                    data_list = json_data['data']
                                    if len(data_list) > 0 and isinstance(data_list[0], list) and len(data_list[0]) >= 2:
                                        # Formato [[fecha, valor], [fecha, valor], ...]
                                        dates = [item[0] for item in data_list]
                                        values = [float(item[1]) for item in data_list]
                                        df = pd.DataFrame({"fecha": dates, "valor": values})
                                        df['fecha_std'] = pd.to_datetime(df['fecha'], errors='coerce')
                                        logger.info(f"✅ BCRA formato data extraído: {len(df)} registros")
                                    else:
                                        logger.error(f"Formato data BCRA no reconocido: {data_list[:3] if data_list else 'vacío'}")
                                        continue
                                else:
                                    # Formato directo como dict
                                    df = pd.DataFrame([json_data])
                                    logger.warning(f"BCRA procesado como dict simple: {len(df)} registros")
                            else:
                                logger.error(f"Formato JSON BCRA no reconocido: {type(json_data)}")
                                continue
                    elif file_info.get("src") == "indec":
                        logger.info(f"📈 Procesando datos INDEC: {file_info['name']}")
                        
                        # INDEC API formato: {"data": [{...}]} o directo
                        if isinstance(json_data, dict) and "data" in json_data:
                            if isinstance(json_data["data"], list):
                                df = pd.DataFrame(json_data["data"])
                            else:
                                df = pd.DataFrame([json_data["data"]])
                        elif isinstance(json_data, list):
                            df = pd.DataFrame(json_data)
                        elif isinstance(json_data, dict):
                            df = pd.DataFrame([json_data])
                        else:
                            logger.error(f"Formato JSON INDEC no reconocido: {type(json_data)}")
                            continue
                    else:
                        # Formato genérico para otras fuentes
                        logger.info(f"📄 Procesando JSON genérico: {file_info['name']}")
                        
                        if isinstance(json_data, list):
                            if len(json_data) > 0:
                                df = pd.DataFrame(json_data)
                            else:
                                logger.warning(f"Lista JSON vacía en {file_info['name']}")
                                continue
                        elif isinstance(json_data, dict):
                            df = pd.DataFrame([json_data])
                        else:
                            logger.warning(f"Formato JSON no reconocido en {file_info['name']}: {type(json_data)}")
                            continue
                        
                        # Intentar crear fecha_std para otros JSONs
                        date_columns = ['fecha', 'date', 'periodo', 'period', 'index']
                        for col in df.columns:
                            if isinstance(col, str) and col.lower() in date_columns:
                                try:
                                    df['fecha_std'] = pd.to_datetime(df[col], errors='coerce')
                                    logger.info(f"✅ fecha_std creada desde {col}")
                                    break
                                except Exception as e:
                                    logger.warning(f"Error convirtiendo {col} a fecha_std: {e}")

                # Verificar que el DataFrame no esté vacío
                if df.empty:
                    logger.warning(f"DataFrame vacío generado para {file_info['name']}")
                    continue
                
                # Filtrar datos desde 2018 en adelante para uniformidad
                if 'fecha_std' in df.columns:
                    original_rows = len(df)
                    valid_dates = df['fecha_std'].notna().sum()
                    logger.info(f"📅 {file_info['name']} - Fechas válidas: {valid_dates}/{original_rows}")
                    
                    if valid_dates > 0:
                        df = df[df['fecha_std'] >= '2018-01-01']
                        filtered_rows = len(df)
                        if original_rows != filtered_rows:
                            logger.info(f"📅 Filtro 2018+: {original_rows} -> {filtered_rows} registros en {file_info['name']}")
                
                # NO FILTRAR POR MENDOZA EN ARCHIVOS ETI - Ya son específicos de Mendoza
                if category in ["turismo"] and not "eti" in file_info.get("name", "").lower():
                    mendoza_keywords = ["mendoza", "mza", "cuyo", "50"]  # Código INDEC Mendoza
                    mendoza_mask = pd.Series([False] * len(df))
                    
                    for col in df.columns:
                        if isinstance(col, str) and df[col].dtype == object:
                            try:
                                mendoza_mask = mendoza_mask | df[col].astype(str).str.contains(
                                    "|".join(mendoza_keywords), case=False, na=False
                                )
                            except Exception as e:
                                logger.warning(f"Error aplicando filtro Mendoza en columna {col}: {e}")
                    
                    if mendoza_mask.any():
                        original_len = len(df)
                        df = df[mendoza_mask]
                        logger.info(f"🗺️ Filtrado Mendoza aplicado a {file_info['name']}: {original_len} -> {len(df)} registros")
                elif "eti" in file_info.get("name", "").lower():
                    logger.info(f"🗺️ ETI {file_info['name']} - Saltando filtro Mendoza (ya es específico de Mendoza)")
                
                # Verificar que aún tengamos datos después de todos los filtros
                if df.empty:
                    logger.warning(f"DataFrame vacío después de filtros para {file_info['name']}")
                    continue
                
                # DEBUG FINAL para archivos procesados
                logger.info(f"🔍 {file_info['name']} FINAL: {len(df)} filas, columnas: {list(df.columns)}")
                logger.info(f"🔍 {file_info['name']} - tiene fecha_std: {'fecha_std' in df.columns}")
                if 'fecha_std' in df.columns:
                    valid_dates = df['fecha_std'].notna().sum()
                    logger.info(f"🔍 {file_info['name']} - fechas válidas: {valid_dates}/{len(df)}")
                
                # Guardar archivo procesado
                output_path = processed_dir / category / f"processed_{file_info['name'].replace('.json', '.csv')}"
                df.to_csv(output_path, index=False, encoding='utf-8')
                
                processed_files[category].append({
                    "original_file": file_info["name"],
                    "processed_path": str(output_path),
                    "rows": len(df),
                    "columns": len(df.columns),
                    "has_date_column": 'fecha_std' in df.columns,
                    "date_range": f"{df['fecha_std'].min()} - {df['fecha_std'].max()}" if 'fecha_std' in df.columns else "N/A",
                    "original_status": file_info.get("status", "unknown"),
                    "data_source": file_info.get("src", "unknown")
                })
                
                logger.info(f"✅ Procesado {category}/{file_info['name']}: {len(df)} filas, {len(df.columns)} columnas")
                
            except Exception as e:
                logger.error(f"❌ Error procesando {file_info['name']}: {e}")
                # Log adicional para debugging
                logger.error(f"   Archivo: {path}")
                logger.error(f"   Categoría: {category}")
                logger.error(f"   Fuente: {file_info.get("src", "unknown")}")
                import traceback
                logger.error(f"   Traceback: {traceback.format_exc()}")
                continue
        
        # Log de resumen final
        logger.info("📊 RESUMEN DE PROCESAMIENTO:")
        for category, files in processed_files.items():
            logger.info(f"  📁 {category}: {len(files)} archivos procesados")
            for file_info in files:
                logger.info(f"    - {file_info['original_file']}: {file_info['rows']} filas, fecha_std: {file_info['has_date_column']}")
        
        # Resumen de procesamiento
        summary = {
            "timestamp": datetime.now().isoformat(),
            "files_by_category": {cat: len(files) for cat, files in processed_files.items()},
            "total_processed": sum(len(files) for files in processed_files.values()),
            "processed_files": processed_files,
            "success": True
        }
        
        # Guardar resumen
        summary_path = processed_dir / "processing_summary.json"
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        
        logger.info(f"📊 Procesamiento completado: {summary['total_processed']} archivos")
        
        return summary
        
    except Exception as e:
        logger.error(f"❌ Error en procesamiento de datos: {e}")
        import traceback
        logger.error(f"Traceback completo: {traceback.format_exc()}")
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
        
        logger.info("🔧 Iniciando creación de matriz de features...")
        logger.info(f"Processing summary recibido: {processing_summary}")
        
        processed_dir = Path(directories["processed"])
        features_dir = Path(directories["features"])
        
        # Diccionario para almacenar series temporales por categoría
        time_series_data = {}
        
        # DEBUG: Verificar qué archivos hay disponibles
        processed_files = processing_summary.get("processed_files", {})
        logger.info(f"📂 Categorías disponibles: {list(processed_files.keys())}")
        for category, files in processed_files.items():
            logger.info(f"📁 {category}: {len(files)} archivos")
            for file_info in files:
                logger.info(f"  - {file_info.get('original_file', 'unknown')}: {file_info.get('rows', 0)} filas")
        
        # Procesar cada categoría de datos
        for category, files in processed_files.items():
            logger.info(f"🔍 Procesando categoría: {category}")
            category_data = []
            
            for file_info in files:
                try:
                    file_path = file_info["processed_path"]
                    logger.info(f"📊 Leyendo archivo: {file_path}")
                    
                    # Verificar que el archivo existe
                    if not Path(file_path).exists():
                        logger.error(f"❌ Archivo no existe: {file_path}")
                        continue
                    
                    df = pd.read_csv(file_path)
                    logger.info(f"📈 Archivo leído: {len(df)} filas, columnas: {list(df.columns)}")
                    
                    if 'fecha_std' in df.columns and not df.empty:
                        logger.info(f"✅ Archivo tiene fecha_std y datos: {file_info['original_file']}")
                        
                        # Mostrar algunas fechas para debug
                        logger.info(f"📅 Primeras fechas: {df['fecha_std'].head()}")
                        
                        df['fecha_std'] = pd.to_datetime(df['fecha_std'], errors='coerce')
                        
                        # Verificar fechas válidas después de conversión
                        valid_dates = df['fecha_std'].notna().sum()
                        logger.info(f"📅 Fechas válidas después de conversión: {valid_dates}/{len(df)}")
                        
                        if valid_dates == 0:
                            logger.warning(f"⚠️ No hay fechas válidas en {file_info['original_file']}")
                            continue
                        
                        # Agregar a nivel mensual
                        df['anio_mes'] = df['fecha_std'].dt.to_period('M')
                        logger.info(f"📅 Períodos únicos: {df['anio_mes'].unique()}")
                        
                        # Identificar columnas numéricas para agregar
                        numeric_cols = df.select_dtypes(include=['number']).columns
                        logger.info(f"🔢 Columnas numéricas encontradas: {list(numeric_cols)}")
                        
                        if len(numeric_cols) > 0:
                            try:
                                monthly_agg = df.groupby('anio_mes')[numeric_cols].agg(['mean', 'sum', 'count']).reset_index()
                                monthly_agg.columns = ['anio_mes'] + [f"{col[0]}_{col[1]}" for col in monthly_agg.columns[1:]]
                                monthly_agg['fuente'] = file_info["original_file"]
                                category_data.append(monthly_agg)
                                
                                logger.info(f"✅ Agregación mensual exitosa: {len(monthly_agg)} períodos")
                                logger.info(f"📊 Columnas agregadas: {list(monthly_agg.columns)}")
                                
                            except Exception as e:
                                logger.error(f"❌ Error en agregación mensual: {e}")
                                continue
                        else:
                            logger.warning(f"⚠️ No hay columnas numéricas para agregar en {file_info['original_file']}")
                    else:
                        if 'fecha_std' not in df.columns:
                            logger.warning(f"⚠️ No hay columna fecha_std en {file_info['original_file']}")
                            logger.info(f"📋 Columnas disponibles: {list(df.columns)}")
                        if df.empty:
                            logger.warning(f"⚠️ DataFrame vacío: {file_info['original_file']}")
                            
                except Exception as e:
                    logger.error(f"❌ Error procesando {file_info.get('processed_path', 'unknown')}: {e}")
                    continue
            
            if category_data:
                # Concatenar todos los datos de la categoría
                logger.info(f"🔗 Concatenando {len(category_data)} DataFrames para categoría {category}")
                category_df = pd.concat(category_data, ignore_index=True)
                time_series_data[category] = category_df
                logger.info(f"✅ Categoría {category} procesada: {len(category_df)} registros")
            else:
                logger.warning(f"⚠️ No se generaron datos para categoría {category}")
        
        if not time_series_data:
            logger.error("❌ No se generaron datos de series temporales")
            logger.info("🔍 Revisando archivos procesados disponibles:")
            
            # Debug adicional: verificar directamente los archivos en disco
            if processed_dir.exists():
                for category_dir in processed_dir.iterdir():
                    if category_dir.is_dir():
                        logger.info(f"📂 Directorio {category_dir.name}:")
                        for file_path in category_dir.iterdir():
                            if file_path.suffix == '.csv':
                                try:
                                    df_test = pd.read_csv(file_path)
                                    logger.info(f"  📄 {file_path.name}: {len(df_test)} filas, columnas: {list(df_test.columns)}")
                                except Exception as e:
                                    logger.error(f"  ❌ Error leyendo {file_path.name}: {e}")
            else:
                logger.error(f"❌ Directorio processed no existe: {processed_dir}")
            
            return ""
        
        logger.info(f"🎯 Datos de series temporales generados para {len(time_series_data)} categorías")
        
        # Crear matriz unificada
        base_periods = None
        unified_features = None
        
        for category, df in time_series_data.items():
            logger.info(f"🔧 Procesando categoría {category} para matriz unificada")
            
            if df.empty:
                logger.warning(f"⚠️ DataFrame vacío para categoría {category}")
                continue
                
            # Crear rango de períodos base
            if base_periods is None:
                min_period = df['anio_mes'].min()
                max_period = df['anio_mes'].max()
                logger.info(f"📅 Rango de períodos: {min_period} a {max_period}")
                base_periods = pd.period_range(start=min_period, end=max_period, freq='M')
                unified_features = pd.DataFrame({'anio_mes': base_periods})
                logger.info(f"📅 Períodos base creados: {len(base_periods)} meses")
            
            # Agregar features de esta categoría
            category_features = df.groupby('anio_mes').agg({
                col: 'mean' for col in df.columns if col not in ['anio_mes', 'fuente']
            }).reset_index()
            
            # Renombrar columnas con prefijo de categoría
            category_features.columns = ['anio_mes'] + [f"{category}_{col}" for col in category_features.columns[1:]]
            
            logger.info(f"🏷️ Features para {category}: {list(category_features.columns)}")
            
            # Merge con matriz unificada
            unified_features = unified_features.merge(category_features, on='anio_mes', how='left')
            logger.info(f"🔗 Merge completado, columnas totales: {len(unified_features.columns)}")
        
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
        
        logger.info(f"🎯 Matriz final: {len(unified_features)} filas x {len(unified_features.columns)} columnas")
        
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
        
        logger.info(f"✅ Matriz de features creada: {len(unified_features)} meses x {len(unified_features.columns)} variables")
        logger.info(f"📅 Rango temporal: {features_metadata['date_range']}")
        logger.info(f"❓ Datos faltantes: {features_metadata['missing_data_percentage']}%")
        
        return str(features_path)
        
    except Exception as e:
        logger.error(f"❌ Error creando matriz de features: {e}")
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
        
        # Convertir fecha_std a datetime si existe
        if 'fecha_std' in df_unified.columns:
            try:
                # Intentar convertir a datetime, reemplazar valores inválidos con NaT
                df_unified['fecha_std'] = pd.to_datetime(df_unified['fecha_std'], errors='coerce')
                
                # Eliminar filas con fechas inválidas
                invalid_dates = df_unified['fecha_std'].isna().sum()
                if invalid_dates > 0:
                    logger.warning(f"Eliminando {invalid_dates} registros con fechas inválidas")
                    df_unified = df_unified.dropna(subset=['fecha_std'])
                
                # Ordenar por fecha
                df_unified = df_unified.sort_values('fecha_std')
                logger.info(f"Dataset ordenado por fecha: {len(df_unified)} registros válidos")
            except Exception as e:
                logger.warning(f"Error procesando fechas: {e}. Continuando sin ordenar por fecha.")
        else:
            logger.info("No se encontró columna fecha_std para ordenar")
        
        # Guardar en volumen local montado
        local_data_dir = Path("/usr/local/airflow/data/raw")
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
            "date_range": f"{df_unified['fecha_std'].min()} - {df_unified['fecha_std'].max()}" if 'fecha_std' in df_unified.columns and not df_unified['fecha_std'].isna().all() else "N/A",
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

@task
def create_final_mendoza_dataset(
    processing_summary: Dict[str, Any],
    directories: Dict[str, str]
) -> str:
    """Crea dataset final trimestral filtrado y limpio para modelado."""
    try:
        logger.info("🎯 Creando dataset final filtrado para modelado...")

        processed_files = processing_summary.get("processed_files", {})
        turismo_files = processed_files.get("turismo", [])
        economico_files = processed_files.get("economico", [])
        logger.info(f"Archivos de turismo procesados: {[f['original_file'] for f in turismo_files]}")
        logger.info(f"Archivos económicos procesados: {[f['original_file'] for f in economico_files]}")

        # DEBUG: Verificar todos los archivos procesados
        for file_info in turismo_files:
            logger.info(f"🔍 Archivo: {file_info.get('original_file', 'unknown')}")
            logger.info(f"   Path: {file_info.get('processed_path', 'unknown')}")
            try:
                df_debug = pd.read_csv(file_info["processed_path"])
                logger.info(f"   Filas: {len(df_debug)}, Columnas: {list(df_debug.columns)}")
                logger.info(f"   Primeras 3 filas:\n{df_debug.head(3)}")
            except Exception as e:
                logger.error(f"   Error leyendo archivo: {e}")

        # Identificar los archivos ETI aeropuerto y cristo redentor
        aeropuerto_file = None
        cristo_file = None
        for file_info in turismo_files:
            fname = file_info.get("original_file", "").lower()
            if "aeropuerto" in fname:
                aeropuerto_file = file_info
                logger.info(f"✅ Aeropuerto identificado: {fname}")
            elif "cristo" in fname:
                cristo_file = file_info
                logger.info(f"✅ Cristo Redentor identificado: {fname}")

        if not aeropuerto_file:
            logger.error("❌ No se encontró archivo ETI aeropuerto.")
            logger.info("Archivos disponibles:")
            for f in turismo_files:
                logger.info(f"  - {f.get('original_file', 'unknown')}")
            return ""

        if not cristo_file:
            logger.error("❌ No se encontró archivo ETI cristo redentor.")
            logger.info("Archivos disponibles:")
            for f in turismo_files:
                logger.info(f"  - {f.get('original_file', 'unknown')}")
            return ""

        # Leer archivos con manejo de errores mejorado
        try:
            df_aeropuerto_full = pd.read_csv(aeropuerto_file["processed_path"])
            logger.info(f"📊 Aeropuerto - Total filas: {len(df_aeropuerto_full)}")
            logger.info(f"📊 Aeropuerto - Columnas: {list(df_aeropuerto_full.columns)}")
            
            if "indice_tiempo" not in df_aeropuerto_full.columns:
                logger.error("❌ Columna 'indice_tiempo' no encontrada en archivo aeropuerto")
                return ""
            if "turistas_no_residentes" not in df_aeropuerto_full.columns:
                logger.error("❌ Columna 'turistas_no_residentes' no encontrada en archivo aeropuerto")
                return ""
                
            df_aeropuerto = df_aeropuerto_full[["indice_tiempo", "turistas_no_residentes"]].copy()
            logger.info(f"📊 Aeropuerto filtrado - Filas: {len(df_aeropuerto)}")
            
        except Exception as e:
            logger.error(f"❌ Error leyendo archivo aeropuerto: {e}")
            return ""

        try:
            df_cristo_full = pd.read_csv(cristo_file["processed_path"])
            logger.info(f"📊 Cristo Redentor - Total filas: {len(df_cristo_full)}")
            logger.info(f"📊 Cristo Redentor - Columnas: {list(df_cristo_full.columns)}")
            
            if "indice_tiempo" not in df_cristo_full.columns:
                logger.error("❌ Columna 'indice_tiempo' no encontrada en archivo cristo redentor")
                return ""
            if "turistas_no_residentes" not in df_cristo_full.columns:
                logger.error("❌ Columna 'turistas_no_residentes' no encontrada en archivo cristo redentor")
                return ""
                
            df_cristo = df_cristo_full[["indice_tiempo", "turistas_no_residentes"]].copy()
            logger.info(f"📊 Cristo Redentor filtrado - Filas: {len(df_cristo)}")
            
        except Exception as e:
            logger.error(f"❌ Error leyendo archivo cristo redentor: {e}")
            return ""

        # Mostrar datos antes de agrupar
        logger.info(f"Aeropuerto antes de agrupar:\n{df_aeropuerto.head()}")
        logger.info(f"Cristo Redentor antes de agrupar:\n{df_cristo.head()}")

        # Agrupar por indice_tiempo
        agg_aeropuerto = df_aeropuerto.groupby("indice_tiempo", as_index=False)["turistas_no_residentes"].sum()
        agg_cristo = df_cristo.groupby("indice_tiempo", as_index=False)["turistas_no_residentes"].sum()

        logger.info(f"Aeropuerto agrupado:\n{agg_aeropuerto}")
        logger.info(f"Cristo Redentor agrupado:\n{agg_cristo}")

        agg_aeropuerto = agg_aeropuerto.rename(columns={"turistas_no_residentes": "turistas_aeropuerto"})
        agg_cristo = agg_cristo.rename(columns={"turistas_no_residentes": "turistas_cristo"})

        # Merge con información detallada
        logger.info("🔗 Realizando merge de aeropuerto y cristo redentor...")
        turistas_agg = pd.merge(agg_aeropuerto, agg_cristo, on="indice_tiempo", how="outer")
        logger.info(f"Resultado del merge:\n{turistas_agg}")
        
        turistas_agg["turistas_aeropuerto"] = turistas_agg["turistas_aeropuerto"].fillna(0)
        turistas_agg["turistas_cristo"] = turistas_agg["turistas_cristo"].fillna(0)
        turistas_agg["turistas_no_residentes_total"] = turistas_agg["turistas_aeropuerto"] + turistas_agg["turistas_cristo"]

        logger.info(f"✅ Suma final por indice_tiempo:\n{turistas_agg}")

        # Verificar que la suma sea correcta
        for _, row in turistas_agg.iterrows():
            aeropuerto_val = row["turistas_aeropuerto"]
            cristo_val = row["turistas_cristo"]
            total_val = row["turistas_no_residentes_total"]
            logger.info(f"  {row['indice_tiempo']}: {aeropuerto_val} + {cristo_val} = {total_val}")

        # --- Procesar datos USD desde archivos BCRA en lugar de scraping ---
        df_dolar_trimestral = pd.DataFrame(columns=["indice_tiempo", "precio_promedio_usd"])
        
        # Buscar archivo BCRA USD procesado
        bcra_usd_file = None
        for file_info in economico_files:
            fname = file_info.get("original_file", "").lower()
            if "usd" in fname or "cotizacion" in fname or "bcra" in fname:
                bcra_usd_file = file_info
                logger.info(f"✅ Archivo BCRA USD identificado: {fname}")
                break
        
        if bcra_usd_file:
            try:
                logger.info(f"💰 Procesando datos USD desde BCRA: {bcra_usd_file['processed_path']}")
                df_bcra = pd.read_csv(bcra_usd_file["processed_path"])
                logger.info(f"📊 BCRA USD - Total filas: {len(df_bcra)}")
                logger.info(f"📊 BCRA USD - Columnas: {list(df_bcra.columns)}")
                logger.info(f"📊 BCRA USD - Primeras 5 filas:\n{df_bcra.head()}")
                
                if 'fecha_std' in df_bcra.columns and 'valor' in df_bcra.columns and not df_bcra.empty:
                    # Convertir fecha_std a datetime si no lo está
                    df_bcra['fecha_std'] = pd.to_datetime(df_bcra['fecha_std'], errors='coerce')
                    
                    # Crear trimestre a partir de fecha
                    df_bcra['year'] = df_bcra['fecha_std'].dt.year
                    df_bcra['quarter'] = df_bcra['fecha_std'].dt.quarter
                    df_bcra['indice_tiempo'] = df_bcra['year'].astype(str) + 'Q' + df_bcra['quarter'].astype(str)
                    
                    # Calcular promedio trimestral
                    df_dolar_trimestral = (

                        df_bcra.groupby("indice_tiempo", as_index=False)["valor"].mean()
                        .rename(columns={"valor": "precio_promedio_usd"})
                    )
                    
                    logger.info(f"✅ Datos USD BCRA procesados: {len(df_dolar_trimestral)} trimestres")
                    logger.info(f"📊 USD trimestral:\n{df_dolar_trimestral}")
                    
                else:
                    logger.warning(f"⚠️ Archivo BCRA no tiene las columnas esperadas: {list(df_bcra.columns)}")
                    
            except Exception as e:
                logger.error(f"❌ Error procesando archivo BCRA USD: {e}")
                logger.error(f"Archivo: {bcra_usd_file['processed_path']}")
        else:
            logger.warning("⚠️ No se encontró archivo BCRA USD procesado")
            logger.info("Archivos económicos disponibles:")
            for f in economico_files:
                logger.info(f"  - {f.get('original_file', 'unknown')}")

        # Preparar dataset final
        df_final = turistas_agg[["indice_tiempo", "turistas_no_residentes_total"]].copy()
        
        # Merge con datos USD
        if not df_dolar_trimestral.empty:
            logger.info(f"🔗 Realizando merge con datos USD")
            df_final = df_final.merge(df_dolar_trimestral, on="indice_tiempo", how="left")
            logger.info(f"✅ Merge USD completado. Datos finales:\n{df_final}")
        else:
            logger.warning("⚠️ No hay datos USD disponibles, agregando columna vacía")
            df_final["precio_promedio_usd"] = None

        # Eventos importantes por trimestre
        eventos = {
            "Q1": ["Fiesta Nacional de la Vendimia", "Vacaciones de Verano"],
            "Q2": [],
            "Q3": ["Vacaciones de Invierno", "Temporada de Esquí"],
            "Q4": ["Fin de Año", "Primavera"]
        }
        def evento_importante(indice_tiempo):
            if isinstance(indice_tiempo, str) and "Q" in indice_tiempo:
                q = indice_tiempo.split("Q")[-1]
                return "; ".join(eventos.get(f"Q{q}", [])) if eventos.get(f"Q{q}", []) else "Sin evento"
            return "Sin evento"
        df_final["evento_importante"] = df_final["indice_tiempo"].apply(evento_importante)

        # Guardar CSV final
        local_data_dir = Path("/usr/local/airflow/data/raw")
        local_data_dir.mkdir(parents=True, exist_ok=True)
        output_path = local_data_dir / "mendoza_turismo_final_filtrado.csv"
        df_final.to_csv(output_path, index=False, encoding="utf-8")

        logger.info(f"✅ CSV final filtrado creado: {output_path} ({len(df_final)} filas)")
        logger.info(f"Columnas: {list(df_final.columns)}")
        logger.info(f"Dataset final:\n{df_final}")
        return str(output_path)

    except Exception as e:
        logger.error(f"❌ Error creando dataset final filtrado: {e}")
        import traceback
        logger.error(f"Traceback completo: {traceback.format_exc()}")
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

    # 7. Crear dataset final específico para modelado (NUEVA TAREA)
    final_dataset = create_final_mendoza_dataset(
        processing_summary=processing_result,
        directories=dirs
    )

    # 8. Validación mejorada
    enhanced_validation = validate_enhanced_data(
        all_downloads=all_downloads,
        processing_summary=processing_result,
        directories=dirs
    )

    # 9. Reporte final mejorado
    final_enhanced_report = generate_enhanced_pipeline_report(
        validation_results=enhanced_validation,
        processing_summary=processing_result,
        features_path=features_matrix,
        directories=dirs
    )

    # Dependencias del pipeline mejorado
    dirs >> all_downloads >> processing_result >> [features_matrix, unified_dataset, final_dataset] >> enhanced_validation >> final_enhanced_report