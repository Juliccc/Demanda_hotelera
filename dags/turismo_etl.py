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
    
    # 2. USD desde argentinadatos.com
    usd_dolarapi_config = cfg.get("usd_dolarapi", {})
    if usd_dolarapi_config and usd_dolarapi_config.get("enabled", True):
        specs.append({
            "src": "dolarapi",
            "name": "usd_historico_dolarapi.json",
            "url": usd_dolarapi_config.get("api_url"),
            "type": "api_json",
            "min_bytes": usd_dolarapi_config.get("min_bytes", 5000),
            "description": "Cotización USD histórica desde argentinadatos.com",
            "category": "economico",
            "params": usd_dolarapi_config.get("params", {})
        })
        logger.info("✅ DolarAPI USD spec configurado")
    
    # 3. Google Trends para "Mendoza"
    google_trends_config = cfg.get("google_trends", {})
    if google_trends_config and google_trends_config.get("enabled", True):
        # Construir URL dinámica con fecha actual
        fecha_actual = datetime.now().strftime('%Y-%m-%d')
        trends_url = f"https://trends.google.es/trends/explore?date=2018-01-01%20{fecha_actual}&geo=AR&q=Mendoza&hl=es"
        
        specs.append({
            "src": "google_trends",
            "name": "mendoza_google_trends_interest.csv",
            "url": trends_url,
            "type": "google_trends_csv",
            "min_bytes": google_trends_config.get("min_bytes", 1000),
            "description": "Interés de búsqueda 'Mendoza' en Google Trends (Argentina)",
            "category": "trends",
            "search_term": "Mendoza",
            "geo": "AR",
            "date_from": "2018-01-01",
            "date_to": fecha_actual
        })
        logger.info("✅ Google Trends Mendoza spec configurado")
    
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
                    
                    # Procesar archivos ETI
                    if "eti" in file_info.get("name", "").lower():
                        logger.info(f"📊 Procesando ETI {file_info['name']}: {len(df)} filas, columnas: {list(df.columns)}")
                        
                        # Buscar columna de fecha
                        fecha_col = None
                        for col in df.columns:
                            if col.lower() in ['fecha', 'periodo', 'period', 'anio_trimestre', 'trimestre']:
                                fecha_col = col
                                break
                        
                        if fecha_col:
                            logger.info(f"✅ ETI {file_info['name']} - Columna de fecha encontrada: {fecha_col}")
                            try:
                                df[fecha_col] = pd.to_datetime(df[fecha_col], errors='coerce')
                                df['indice_tiempo'] = df[fecha_col]
                                df['fecha_std'] = df[fecha_col]
                                logger.info(f"✅ ETI {file_info['name']} - fecha_std creada exitosamente")
                            except Exception as e:
                                logger.error(f"❌ ETI {file_info['name']} - Error creando fecha_std: {e}")
                        
                elif path.endswith(".json"):
                    with open(path, 'r', encoding='utf-8') as f:
                        json_data = json.load(f)
                    
                    # Procesar datos de argentinadatos.com (USD)
                    if file_info.get("src") == "dolarapi" or "argentinadatos" in file_info.get("name", ""):
                        logger.info(f"💰 Procesando datos USD desde argentinadatos.com: {file_info['name']}")
                        
                        if isinstance(json_data, list) and len(json_data) > 0:
                            df = pd.DataFrame(json_data)
                            
                            # Identificar columna de fecha de manera flexible
                            fecha_col = None
                            for col in df.columns:
                                if col.lower() in ['fecha', 'date', 'time', 'timestamp']:
                                    fecha_col = col
                                    break
                            
                            if fecha_col:
                                df['fecha_std'] = pd.to_datetime(df[fecha_col], errors='coerce')
                                logger.info(f"✅ USD DataFrame creado: {len(df)} registros válidos con fecha_std desde '{fecha_col}'")
                            else:
                                logger.error(f"❌ No se encontró columna de fecha. Columnas: {list(df.columns)}")
                                continue
                        else:
                            logger.error("❌ Formato JSON USD no válido")
                            continue

                # Verificar que el DataFrame no esté vacío
                if df.empty:
                    logger.warning(f"DataFrame vacío generado para {file_info['name']}")
                    continue
                
                # Filtrar datos desde 2018 en adelante
                if 'fecha_std' in df.columns:
                    original_rows = len(df)
                    valid_dates = df['fecha_std'].notna().sum()
                    logger.info(f"📅 {file_info['name']} - Fechas válidas: {valid_dates}/{original_rows}")
                    
                    if valid_dates > 0:
                        df = df[df['fecha_std'] >= '2018-01-01']
                        filtered_rows = len(df)
                        if original_rows != filtered_rows:
                            logger.info(f"📅 Filtro 2018+: {original_rows} -> {filtered_rows} registros en {file_info['name']}")
                
                # Verificar que aún tengamos datos después de filtros
                if df.empty:
                    logger.warning(f"DataFrame vacío después de filtros para {file_info['name']}")
                    continue
                
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
                continue
        
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
        return {"error": str(e), "success": False}

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
                    "turismo": validation_results.get("model_readiness", {}).get("has_tourism_data", False),
                    "economic": validation_results.get("model_readiness", {}).get("has_economic_data", False)
                }
            },
            "next_steps": [
                "Dataset multi-dimensional listo para análisis exploratorio avanzado",
                "Variables económicas y estacionales incorporadas",
                "Preparar notebook para EDA con correlaciones entre variables",
                "Implementar modelos de serie temporal (ARIMA, Prophet, LSTM)"
            ],
            "recommendations": []
        }
        
        # Generar recomendaciones basadas en resultados
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
        
        logger.info("=" * 70)
        logger.info("PIPELINE MEJORADO DE DEMANDA HOTELERA COMPLETADO")
        logger.info("=" * 70)
        logger.info(f"Archivos procesados: {successful_files}/{total_files} ({success_rate}%)")
        logger.info(f"Categorías de datos: {', '.join(pipeline_report['data_summary']['categories_processed'])}")
        logger.info(f"Variables económicas: {'SI' if pipeline_report['data_summary']['critical_data_available']['economic'] else 'NO'}")
        logger.info(f"Directorio de datos: {directories['base']}")
        logger.info(f"Reporte completo: {report_path.name}")
        logger.info("LISTO PARA MODELADO PREDICTIVO AVANZADO")
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
def download_usd_historical_dolarapi(directories: dict) -> dict:
    """
    Descarga datos históricos del dólar desde 
    https://api.argentinadatos.com/v1/cotizaciones/dolares/
    y filtra por dólar oficial desde 2018-01-01 hasta la fecha actual.
    """
    try:
        # Nueva URL que devuelve todo el histórico
        url = "https://api.argentinadatos.com/v1/cotizaciones/dolares/"
        
        headers = {
            "User-Agent": "TurismoDataPipeline/2.0",
            "Accept": "application/json"
        }
        
        logger.info(f"🔄 Descargando histórico completo del dólar desde argentinadatos.com")
        logger.info(f"🔗 URL: {url}")
        
        response = requests.get(url, headers=headers, timeout=120)
        response.raise_for_status()
        
        data = response.json()
        
        if not isinstance(data, list):
            logger.error(f"❌ Formato de respuesta inesperado: {type(data)}")
            return {"status": "error", "error": "Formato de respuesta inesperado"}
        
        if len(data) == 0:
            logger.error("❌ No se recibieron datos USD")
            return {"status": "error", "error": "No se recibieron datos"}
        
        logger.info(f"📊 Datos totales recibidos: {len(data)} registros")
        
        # Filtrar por dólar oficial y fecha desde 2018-01-01
        fecha_inicio = datetime(2018, 1, 1)
        fecha_actual = datetime.now()
        
        datos_filtrados = []
        
        for record in data:
            # Verificar que sea dólar oficial
            casa = record.get('casa', '').lower()
            if 'oficial' not in casa:
                continue
            
            # Verificar fecha
            fecha_str = record.get('fecha')
            if not fecha_str:
                continue
            
            try:
                # Parsear fecha (formato: YYYY-MM-DD)
                fecha = datetime.strptime(fecha_str, '%Y-%m-%d')
                
                # Filtrar por rango de fechas
                if fecha >= fecha_inicio and fecha <= fecha_actual:
                    datos_filtrados.append(record)
            except ValueError:
                # Si no se puede parsear la fecha, intentar otros formatos
                try:
                    fecha = datetime.fromisoformat(fecha_str.replace('Z', '+00:00'))
                    if fecha >= fecha_inicio and fecha <= fecha_actual:
                        datos_filtrados.append(record)
                except:
                    continue
        
        if len(datos_filtrados) == 0:
            logger.error("❌ No se encontraron datos del dólar oficial desde 2018-01-01")
            return {"status": "error", "error": "No hay datos del dólar oficial en el rango de fechas"}
        
        logger.info(f"✅ Datos filtrados del dólar oficial: {len(datos_filtrados)} registros")
        
        # Validar estructura de datos
        sample_record = datos_filtrados[0]
        logger.info(f"📋 Estructura de datos de muestra: {list(sample_record.keys())}")
        
        # Guardar datos raw filtrados
        raw_dir = Path(directories["raw"]) / "economico"
        raw_dir.mkdir(parents=True, exist_ok=True)
        dest_path = raw_dir / "usd_historico_argentinadatos.json"
        
        with open(dest_path, "w", encoding="utf-8") as f:
            json.dump(datos_filtrados, f, indent=2, ensure_ascii=False)
        
        # Análisis de datos filtrados
        dates = [record.get('fecha') for record in datos_filtrados if record.get('fecha')]
        date_range = f"{min(dates)} - {max(dates)}" if dates else "N/A"
        
        logger.info(f"✅ Datos USD oficiales filtrados guardados: {len(datos_filtrados)} registros")
        logger.info(f"📊 Rango de fechas filtrado: {date_range}")
        logger.info(f"📋 Campos en cada registro: {list(sample_record.keys())}")
        
        return {
            "status": "downloaded",
            "path": str(dest_path),
            "records": len(datos_filtrados),
            "data": datos_filtrados,
            "date_range": date_range,
            "api_source": "argentinadatos.com",
            "filter_applied": "dolar_oficial_desde_2018"
        }
        
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Error de conexión con argentinadatos.com: {e}")
        return {"status": "error", "error": f"Error de conexión: {str(e)}"}
    
    except json.JSONDecodeError as e:
        logger.error(f"❌ Error decodificando JSON: {e}")
        return {"status": "error", "error": f"Error JSON: {str(e)}"}
    
    except Exception as e:
        logger.error(f"❌ Error descargando USD desde argentinadatos.com: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return {"status": "error", "error": str(e)}

@task
def process_usd_to_quarterly_averages(
    usd_data: dict,
    directories: dict
) -> dict:
    """
    Procesa los datos históricos del dólar para obtener promedios trimestrales y variables derivadas.
    Adaptado para argentinadatos.com API.
    """
    try:
        if usd_data.get("status") != "downloaded":
            logger.error("No se puede procesar datos USD: descarga fallida")
            return {"status": "error"}
        
        data = usd_data["data"]
        df = pd.DataFrame(data)
        
        logger.info(f"📊 Procesando {len(df)} registros de USD desde {usd_data.get('api_source', 'API')}")
        logger.info(f"📋 Columnas disponibles: {list(df.columns)}")
        
        # Identificar columnas de fecha y precio de venta
        fecha_col = None
        venta_col = None
        
        # Buscar columna de fecha
        for col in df.columns:
            if col.lower() in ['fecha', 'date', 'time', 'timestamp']:
                fecha_col = col
                break
        
        # Buscar columna de precio de venta
        for col in df.columns:
            if col.lower() in ['venta', 'sell', 'precio_venta', 'valor']:
                venta_col = col
                break
        
        if not fecha_col:
            logger.error(f"❌ No se encontró columna de fecha. Columnas: {list(df.columns)}")
            return {"status": "error", "error": "Columna de fecha no encontrada"}
        
        if not venta_col:
            logger.error(f"❌ No se encontró columna de venta. Columnas: {list(df.columns)}")
            return {"status": "error", "error": "Columna de venta no encontrada"}
        
        logger.info(f"✅ Usando columna fecha: '{fecha_col}', venta: '{venta_col}'")
        
        # Procesar fechas
        df["fecha"] = pd.to_datetime(df[fecha_col], errors="coerce")
        df = df[df["fecha"].notna()]
        df = df[df["fecha"] >= "2018-01-01"]
        
        # Asegurar que venta sea numérico
        df["venta"] = pd.to_numeric(df[venta_col], errors="coerce")
        df = df[df["venta"].notna()]
        
        logger.info(f"📅 Datos después de limpieza: {len(df)} registros")
        logger.info(f"📊 Rango USD: ${df['venta'].min():.2f} - ${df['venta'].max():.2f}")
        
        # Crear índice trimestral
        df["anio_trimestre"] = df["fecha"].dt.year.astype(str) + "Q" + df["fecha"].dt.quarter.astype(str)
        
        # Agregación trimestral - solo precio promedio y días
        df_quarterly = df.groupby("anio_trimestre").agg(
            precio_promedio_usd=("venta", "mean"),
            dias=("venta", "count")
        ).reset_index()
        
        # Renombrar columna para merge con turismo
        df_quarterly = df_quarterly.rename(columns={"anio_trimestre": "indice_tiempo"})
        
        # Redondear valores
        df_quarterly["precio_promedio_usd"] = df_quarterly["precio_promedio_usd"].round(2)
        
        # Guardar CSV procesado
        processed_dir = Path(directories["processed"]) / "economico"
        processed_dir.mkdir(parents=True, exist_ok=True)
        simple_path = processed_dir / "usd_quarterly_argentinadatos.csv"
        df_quarterly.to_csv(simple_path, index=False, encoding="utf-8")
        
        logger.info(f"✅ USD trimestral procesado: {len(df_quarterly)} trimestres")
        logger.info(f"📊 Rango temporal: {df_quarterly['indice_tiempo'].min()} - {df_quarterly['indice_tiempo'].max()}")
        logger.info(f"💰 Precio promedio general: ${df_quarterly['precio_promedio_usd'].mean():.2f}")
        
        return {
            "status": "processed",
            "simple_path": str(simple_path),
            "records": len(df_quarterly),
            "date_range": f"{df_quarterly['indice_tiempo'].min()} - {df_quarterly['indice_tiempo'].max()}",
            "avg_usd_price": round(df_quarterly['precio_promedio_usd'].mean(), 2)
        }
    except Exception as e:
        logger.error(f"❌ Error procesando USD trimestral: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return {"status": "error", "error": str(e)}

@task
def download_google_trends_csv(
    spec: Dict[str, Any],
    directories: Dict[str, str]
) -> Dict[str, Any]:
    """Descarga datos de Google Trends para el término 'Mendoza' desde 2018."""
    try:
        src = spec["src"]
        name = spec["name"] 
        search_term = spec.get("search_term", "Mendoza")
        geo = spec.get("geo", "AR")
        date_from = spec.get("date_from", "2018-01-01")
        date_to = spec.get("date_to", datetime.now().strftime('%Y-%m-%d'))
        category = spec.get("category", "trends")
        
        raw_dir = Path(directories["raw"]) / category
        raw_dir.mkdir(parents=True, exist_ok=True)
        dest_path = raw_dir / name
        
        logger.info(f"📈 Descargando Google Trends para '{search_term}' desde {date_from} hasta {date_to}")
        
        try:
            # Importar pytrends si está disponible
            from pytrends.request import TrendReq
            
            # Configurar pytrends
            pytrends = TrendReq(hl='es', tz=360)
            
            # Construir timeframe para pytrends (formato: YYYY-MM-DD YYYY-MM-DD)
            timeframe = f"{date_from} {date_to}"
            
            # Realizar búsqueda
            logger.info(f"🔍 Buscando tendencia para: {search_term} en {geo} durante {timeframe}")
            pytrends.build_payload([search_term], cat=0, timeframe=timeframe, geo=geo, gprop='')
            
            # Obtener datos de interés a lo largo del tiempo
            interest_over_time_df = pytrends.interest_over_time()
            
            if interest_over_time_df.empty:
                logger.error(f"❌ No se obtuvieron datos de Google Trends para {search_term}")
                return {
                    "src": src, "name": name, "path": "", "size": 0, 
                    "status": "error", "error": "No data from Google Trends",
                    "category": category
                }
            
            # Limpiar datos (remover columna 'isPartial' si existe)
            if 'isPartial' in interest_over_time_df.columns:
                interest_over_time_df = interest_over_time_df.drop(columns=['isPartial'])
            
            # Renombrar columna de interés
            if search_term in interest_over_time_df.columns:
                interest_over_time_df = interest_over_time_df.rename(columns={search_term: 'interes_google'})
            
            # Resetear índice para tener fecha como columna
            interest_over_time_df = interest_over_time_df.reset_index()
            
            # Asegurar que la columna de fecha se llame 'fecha'
            if 'date' in interest_over_time_df.columns:
                interest_over_time_df = interest_over_time_df.rename(columns={'date': 'fecha'})
            
            # Guardar CSV
            interest_over_time_df.to_csv(dest_path, index=False, encoding='utf-8')
            
            file_size = dest_path.stat().st_size
            
            logger.info(f"✅ Google Trends descargado: {len(interest_over_time_df)} registros mensuales")
            logger.info(f"📊 Rango de interés: {interest_over_time_df['interes_google'].min()} - {interest_over_time_df['interes_google'].max()}")
            logger.info(f"📅 Período: {interest_over_time_df['fecha'].min()} - {interest_over_time_df['fecha'].max()}")
            
            return {
                "src": src, "name": name, "path": str(dest_path),
                "size": file_size, "status": "downloaded", 
                "description": spec["description"], "category": category,
                "records_count": len(interest_over_time_df),
                "search_term": search_term,
                "date_range": f"{interest_over_time_df['fecha'].min()} - {interest_over_time_df['fecha'].max()}"
            }
            
        except ImportError:
            logger.error("❌ pytrends no está instalado. Intentando descarga manual desde URL.")
            
            # Fallback: intentar descarga directa (aunque Google Trends no suele permitir esto)
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
            
            # NOTA: Esta URL probablemente no funcionará directamente
            # Google Trends requiere autenticación y tokens
            fallback_url = spec.get("url", "")
            
            logger.warning("⚠️ Método de fallback no recomendado. Instalar pytrends para funcionalidad completa.")
            return {
                "src": src, "name": name, "path": "", "size": 0,
                "status": "error", "error": "pytrends not available and fallback not implemented",
                "category": category, "recommendation": "pip install pytrends"
            }
            
    except Exception as e:
        logger.error(f"❌ Error descargando Google Trends: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return {
            "src": spec.get("src", "unknown"),
            "name": spec.get("name", "unknown"),
            "path": "", "size": 0, "status": "error",
            "error": str(e)[:200], "category": spec.get("category", "unknown")
        }

@task(execution_timeout=timedelta(minutes=10))
def process_google_trends_to_quarterly(
    trends_data: dict,
    directories: dict
) -> dict:
    """
    Procesa datos mensuales de Google Trends a promedios trimestrales.
    """
    try:
        if trends_data.get("status") != "downloaded":
            logger.error("No se puede procesar Google Trends: descarga fallida")
            return {"status": "error", "error": "Trends download failed"}
        
        trends_path = trends_data["path"]
        
        if not Path(trends_path).exists():
            logger.error(f"Archivo de Google Trends no existe: {trends_path}")
            return {"status": "error", "error": "Trends file not found"}
        
        # Leer datos de Google Trends
        df_trends = pd.read_csv(trends_path)
        
        logger.info(f"📈 Procesando Google Trends: {len(df_trends)} registros mensuales")
        logger.info(f"📋 Columnas disponibles: {list(df_trends.columns)}")
        
        # Buscar columnas de fecha e interés
        fecha_col = None
        interes_col = None
        
        for col in df_trends.columns:
            if col.lower() in ['fecha', 'date', 'time', 'timestamp']:
                fecha_col = col
                break
        
        for col in df_trends.columns:
            if 'interes' in col.lower() or 'interest' in col.lower() or col == 'Mendoza':
                interes_col = col
                break
        
        if not fecha_col:
            logger.error(f"❌ No se encontró columna de fecha. Columnas: {list(df_trends.columns)}")
            return {"status": "error", "error": "Date column not found"}
        
        if not interes_col:
            logger.error(f"❌ No se encontró columna de interés. Columnas: {list(df_trends.columns)}")
            return {"status": "error", "error": "Interest column not found"}
        
        logger.info(f"✅ Usando columnas - Fecha: '{fecha_col}', Interés: '{interes_col}'")
        
        # Procesar fechas
        df_trends["fecha"] = pd.to_datetime(df_trends[fecha_col], errors="coerce")
        df_trends = df_trends[df_trends["fecha"].notna()]
        df_trends = df_trends[df_trends["fecha"] >= "2018-01-01"]
        
        # Asegurar que el interés sea numérico
        df_trends["interes_google"] = pd.to_numeric(df_trends[interes_col], errors="coerce")
        df_trends = df_trends[df_trends["interes_google"].notna()]
        
        logger.info(f"📅 Datos después de limpieza: {len(df_trends)} registros")
        logger.info(f"📊 Rango de interés: {df_trends['interes_google'].min()} - {df_trends['interes_google'].max()}")
        
        # Crear índice trimestral
        df_trends["anio_trimestre"] = (
            df_trends["fecha"].dt.year.astype(str) + "Q" + 
            df_trends["fecha"].dt.quarter.astype(str)
        )
        
        # Agregación trimestral
        df_quarterly = df_trends.groupby("anio_trimestre").agg(
            interes_google_promedio=("interes_google", "mean"),
            interes_google_max=("interes_google", "max"),
            interes_google_min=("interes_google", "min"),
            meses_con_datos=("interes_google", "count")
        ).reset_index()
        
        # Renombrar columna para merge
        df_quarterly = df_quarterly.rename(columns={"anio_trimestre": "indice_tiempo"})
        
        # Redondear valores
        df_quarterly["interes_google_promedio"] = df_quarterly["interes_google_promedio"].round(1)
        
        # Crear variable de alto interés (por encima de la mediana)
        mediana_interes = df_quarterly["interes_google_promedio"].median()
        df_quarterly["interes_alto"] = (df_quarterly["interes_google_promedio"] > mediana_interes).astype(int)
        
        # Guardar CSV procesado
        processed_dir = Path(directories["processed"]) / "trends"
        processed_dir.mkdir(parents=True, exist_ok=True)
        trends_quarterly_path = processed_dir / "google_trends_mendoza_quarterly.csv"
        df_quarterly.to_csv(trends_quarterly_path, index=False, encoding="utf-8")
        
        logger.info(f"✅ Google Trends trimestral procesado: {len(df_quarterly)} trimestres")
        logger.info(f"📊 Rango temporal: {df_quarterly['indice_tiempo'].min()} - {df_quarterly['indice_tiempo'].max()}")
        logger.info(f"📈 Interés promedio general: {df_quarterly['interes_google_promedio'].mean():.1f}")
        logger.info(f"🎯 Mediana de interés: {mediana_interes:.1f}")
        
        return {
            "status": "processed",
            "quarterly_path": str(trends_quarterly_path),
            "records": len(df_quarterly),
            "date_range": f"{df_quarterly['indice_tiempo'].min()} - {df_quarterly['indice_tiempo'].max()}",
            "avg_interest": round(df_quarterly['interes_google_promedio'].mean(), 1),
            "median_interest": round(mediana_interes, 1)
        }
        
    except Exception as e:
        logger.error(f"❌ Error procesando Google Trends trimestral: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return {"status": "error", "error": str(e)}

@task
def create_final_mendoza_dataset(
    processing_summary: Dict[str, Any],
    usd_quarterly: Dict[str, Any],
    trends_quarterly: Dict[str, Any],  # Nuevo parámetro
    directories: Dict[str, str]
) -> str:
    """Crea dataset final trimestral con USD y Google Trends sincronizados."""
    try:
        logger.info("🎯 Creando dataset final con USD y Google Trends...")

        processed_files = processing_summary.get("processed_files", {})
        turismo_files = processed_files.get("turismo", [])
        
        # Identificar archivos ETI
        aeropuerto_file = None
        cristo_file = None
        for file_info in turismo_files:
            fname = file_info.get("original_file", "").lower()
            if "aeropuerto" in fname:
                aeropuerto_file = file_info
            elif "cristo" in fname:
                cristo_file = file_info

        if not aeropuerto_file or not cristo_file:
            logger.error("❌ No se encontraron archivos ETI necesarios")
            return ""

        # Procesar datos de turismo
        df_aeropuerto_full = pd.read_csv(aeropuerto_file["processed_path"])
        df_cristo_full = pd.read_csv(cristo_file["processed_path"])
        
        # DEBUG: Verificar columnas disponibles
        logger.info(f"🔍 Columnas aeropuerto: {list(df_aeropuerto_full.columns)}")
        logger.info(f"🔍 Columnas cristo: {list(df_cristo_full.columns)}")
        
        # Buscar columnas de turistas de manera más flexible
        turistas_col_aeropuerto = None
        turistas_col_cristo = None
        indice_col_aeropuerto = None
        indice_col_cristo = None
        
        # Buscar columnas de turistas
        for col in df_aeropuerto_full.columns:
            if 'turistas' in col.lower() and ('no_residentes' in col.lower() or 'residentes' in col.lower()):
                turistas_col_aeropuerto = col
                break
        
        for col in df_cristo_full.columns:
            if 'turistas' in col.lower() and ('no_residentes' in col.lower() or 'residentes' in col.lower()):
                turistas_col_cristo = col
                break
        
        # Buscar columnas de índice temporal
        for col in df_aeropuerto_full.columns:
            if col.lower() in ['indice_tiempo', 'fecha_std', 'periodo', 'anio_trimestre']:
                indice_col_aeropuerto = col
                break
        
        for col in df_cristo_full.columns:
            if col.lower() in ['indice_tiempo', 'fecha_std', 'periodo', 'anio_trimestre']:
                indice_col_cristo = col
                break
        
        logger.info(f"✅ Columnas identificadas - Aeropuerto: {indice_col_aeropuerto}, {turistas_col_aeropuerto}")
        logger.info(f"✅ Columnas identificadas - Cristo: {indice_col_cristo}, {turistas_col_cristo}")
        
        if not turistas_col_aeropuerto or not turistas_col_cristo:
            logger.error("❌ No se encontraron columnas de turistas")
            return ""
        
        if not indice_col_aeropuerto or not indice_col_cristo:
            logger.error("❌ No se encontraron columnas de índice temporal")
            return ""
        
        # Extraer datos necesarios
        df_aeropuerto = df_aeropuerto_full[[indice_col_aeropuerto, turistas_col_aeropuerto]].copy()
        df_cristo = df_cristo_full[[indice_col_cristo, turistas_col_cristo]].copy()
        
        # Estandarizar nombres de columnas
        df_aeropuerto.columns = ["indice_tiempo", "turistas_no_residentes"]
        df_cristo.columns = ["indice_tiempo", "turistas_no_residentes"]
        
        # CONVERTIR FECHAS A FORMATO TRIMESTRAL ANTES DE AGRUPAR
        def convertir_fecha_a_trimestre(fecha_str):
            """Convierte fecha '2018-01-01' a formato '2018Q1'"""
            try:
                if pd.isna(fecha_str):
                    return None
                fecha = pd.to_datetime(fecha_str)
                año = fecha.year
                trimestre = fecha.quarter
                return f"{año}Q{trimestre}"
            except:
                return None
        
        # Aplicar conversión de formato
        df_aeropuerto['indice_tiempo_original'] = df_aeropuerto['indice_tiempo']
        df_cristo['indice_tiempo_original'] = df_cristo['indice_tiempo']
        
        df_aeropuerto['indice_tiempo'] = df_aeropuerto['indice_tiempo'].apply(convertir_fecha_a_trimestre)
        df_cristo['indice_tiempo'] = df_cristo['indice_tiempo'].apply(convertir_fecha_a_trimestre)
        
        # Filtrar valores nulos después de conversión
        df_aeropuerto = df_aeropuerto[df_aeropuerto['indice_tiempo'].notna()]
        df_cristo = df_cristo[df_cristo['indice_tiempo'].notna()]
        
        logger.info(f"🔄 Conversión completada - Aeropuerto: {len(df_aeropuerto)} registros")
        logger.info(f"🔄 Conversión completada - Cristo: {len(df_cristo)} registros")
        logger.info(f"🔍 Muestra índices convertidos: {df_aeropuerto['indice_tiempo'].head().tolist()}")
        
        # Convertir a numérico
        df_aeropuerto["turistas_no_residentes"] = pd.to_numeric(df_aeropuerto["turistas_no_residentes"], errors='coerce').fillna(0)
        df_cristo["turistas_no_residentes"] = pd.to_numeric(df_cristo["turistas_no_residentes"], errors='coerce').fillna(0)

        # Agrupar por indice_tiempo
        agg_aeropuerto = df_aeropuerto.groupby("indice_tiempo", as_index=False)["turistas_no_residentes"].sum()
        agg_cristo = df_cristo.groupby("indice_tiempo", as_index=False)["turistas_no_residentes"].sum()

        agg_aeropuerto = agg_aeropuerto.rename(columns={"turistas_no_residentes": "turistas_aeropuerto"})
        agg_cristo = agg_cristo.rename(columns={"turistas_no_residentes": "turistas_cristo"})

        # Merge datos de turismo
        turistas_agg = pd.merge(agg_aeropuerto, agg_cristo, on="indice_tiempo", how="outer")
        turistas_agg["turistas_aeropuerto"] = turistas_agg["turistas_aeropuerto"].fillna(0)
        turistas_agg["turistas_cristo"] = turistas_agg["turistas_cristo"].fillna(0)
        turistas_agg["turistas_no_residentes_total"] = turistas_agg["turistas_aeropuerto"] + turistas_agg["turistas_cristo"]

        logger.info(f"✅ Datos de turismo procesados: {len(turistas_agg)} trimestres")
        logger.info(f"🔍 Muestra índice_tiempo turismo CONVERTIDO: {sorted(turistas_agg['indice_tiempo'].unique())[:5]}")

        # Preparar dataset final con datos de turismo
        df_final = turistas_agg[["indice_tiempo", "turistas_no_residentes_total", "turistas_aeropuerto", "turistas_cristo"]].copy()

        # Merge con datos USD procesados desde argentinadatos.com
        if usd_quarterly.get("status") == "processed":
            logger.info("💰 Mergeando con datos USD trimestrales desde argentinadatos.com...")
            
            # Leer datos USD trimestrales
            usd_path = usd_quarterly["simple_path"]
            logger.info(f"📁 Leyendo USD desde: {usd_path}")
            
            if not Path(usd_path).exists():
                logger.error(f"❌ Archivo USD no existe: {usd_path}")
                df_final["precio_promedio_usd"] = None
            else:
                df_usd = pd.read_csv(usd_path)
                
                logger.info(f"📊 Datos USD leídos: {len(df_usd)} trimestres")
                logger.info(f"📋 Columnas USD: {list(df_usd.columns)}")
                logger.info(f"🔍 Muestra índice_tiempo USD: {sorted(df_usd['indice_tiempo'].unique())[:5]}")
                logger.info(f"💰 Muestra precios USD: {df_usd['precio_promedio_usd'].head().tolist()}")
                
                # DEBUG: Verificar tipos de datos antes del merge
                logger.info(f"🔍 Tipo indice_tiempo turismo: {df_final['indice_tiempo'].dtype}")
                logger.info(f"🔍 Tipo indice_tiempo USD: {df_usd['indice_tiempo'].dtype}")
                
                # Asegurar que ambos índices sean string para el merge
                df_final['indice_tiempo'] = df_final['indice_tiempo'].astype(str)
                df_usd['indice_tiempo'] = df_usd['indice_tiempo'].astype(str)
                
                # Realizar merge
                df_final_before_merge = df_final.copy()
                df_final = df_final.merge(df_usd[['indice_tiempo', 'precio_promedio_usd']], on="indice_tiempo", how="left")
                
                # Verificar resultado del merge
                usd_matches = df_final['precio_promedio_usd'].notna().sum()
                total_rows = len(df_final)
                
                logger.info(f"✅ Merge USD completado: {usd_matches}/{total_rows} trimestres con datos USD")
                
                if usd_matches == 0:
                    logger.error("❌ PROBLEMA: Ningún registro USD se mergeó correctamente")
                    logger.info("🔍 Diagnóstico del merge:")
                    logger.info(f"   - Índices únicos turismo: {sorted(df_final_before_merge['indice_tiempo'].unique())}")
                    logger.info(f"   - Índices únicos USD: {sorted(df_usd['indice_tiempo'].unique())}")
                    
                    # Intentar merge manual para debug
                    common_indices = set(df_final_before_merge['indice_tiempo'].unique()) & set(df_usd['indice_tiempo'].unique())
                    logger.info(f"   - Índices en común: {sorted(common_indices)}")
                    
                    if len(common_indices) == 0:
                        logger.error("   - NO HAY ÍNDICES EN COMÚN - revisar formato de fechas")
                else:
                    logger.info(f"✅ MERGE EXITOSO: {usd_matches} trimestres con datos USD sincronizados")
                    
                # Crear variables derivadas del USD solo si hay datos
                if usd_matches > 0:
                    median_usd = df_final['precio_promedio_usd'].median()
                    df_final['usd_alto'] = (df_final['precio_promedio_usd'] > median_usd).astype(int)
                    logger.info(f"💰 Variable usd_alto creada (mediana: ${median_usd:.2f})")
                else:
                    df_final['usd_alto'] = None
                    logger.warning("⚠️ No se pudo crear variable usd_alto por falta de datos USD")
            
        else:
            logger.warning("⚠️ No hay datos USD procesados disponibles")
            df_final["precio_promedio_usd"] = None
            df_final['usd_alto'] = None

        # NUEVO: Merge con datos de Google Trends
        if trends_quarterly.get("status") == "processed":
            logger.info("📈 Mergeando con datos de Google Trends trimestrales...")
            
            trends_path = trends_quarterly["quarterly_path"]
            logger.info(f"📁 Leyendo Google Trends desde: {trends_path}")
            
            if not Path(trends_path).exists():
                logger.error(f"❌ Archivo Google Trends no existe: {trends_path}")
                df_final["interes_google_promedio"] = None
                df_final["interes_alto"] = None
            else:
                df_trends = pd.read_csv(trends_path)
                
                logger.info(f"📊 Datos Google Trends leídos: {len(df_trends)} trimestres")
                logger.info(f"📋 Columnas Google Trends: {list(df_trends.columns)}")
                logger.info(f"🔍 Muestra índice_tiempo Trends: {sorted(df_trends['indice_tiempo'].unique())[:5]}")
                logger.info(f"📈 Muestra interés: {df_trends['interes_google_promedio'].head().tolist()}")
                
                # Asegurar que ambos índices sean string para el merge
                df_final['indice_tiempo'] = df_final['indice_tiempo'].astype(str)
                df_trends['indice_tiempo'] = df_trends['indice_tiempo'].astype(str)
                
                # Realizar merge con Google Trends
                df_final_before_trends = df_final.copy()
                df_final = df_final.merge(
                    df_trends[['indice_tiempo', 'interes_google_promedio', 'interes_alto']], 
                    on="indice_tiempo", 
                    how="left"
                )
                
                # Verificar resultado del merge
                trends_matches = df_final['interes_google_promedio'].notna().sum()
                total_rows = len(df_final)
                
                logger.info(f"✅ Merge Google Trends completado: {trends_matches}/{total_rows} trimestres con datos de interés")
                
                if trends_matches == 0:
                    logger.error("❌ PROBLEMA: Ningún registro de Google Trends se mergeó correctamente")
                    logger.info("🔍 Diagnóstico del merge Google Trends:")
                    logger.info(f"   - Índices únicos turismo: {sorted(df_final_before_trends['indice_tiempo'].unique())}")
                    logger.info(f"   - Índices únicos Trends: {sorted(df_trends['indice_tiempo'].unique())}")
                    
                    common_indices = set(df_final_before_trends['indice_tiempo'].unique()) & set(df_trends['indice_tiempo'].unique())
                    logger.info(f"   - Índices en común: {sorted(common_indices)}")
                else:
                    logger.info(f"✅ MERGE GOOGLE TRENDS EXITOSO: {trends_matches} trimestres con datos de interés sincronizados")
        
        else:
            logger.warning("⚠️ No hay datos de Google Trends procesados disponibles")
            df_final["interes_google_promedio"] = None
            df_final["interes_alto"] = None

        # Agregar variables temporales y eventos
        def extraer_año_trimestre(indice_tiempo):
            if isinstance(indice_tiempo, str) and "Q" in indice_tiempo:
                año, q = indice_tiempo.split("Q")
                return int(año), int(q)
            return None, None

        df_final[['año', 'trimestre']] = df_final['indice_tiempo'].apply(
            lambda x: pd.Series(extraer_año_trimestre(x))
        )

        # Variables estacionales
        df_final['es_verano'] = (df_final['trimestre'] == 1).astype(int)
        df_final['es_otoño'] = (df_final['trimestre'] == 2).astype(int)
        df_final['es_invierno'] = (df_final['trimestre'] == 3).astype(int)
        df_final['es_primavera'] = (df_final['trimestre'] == 4).astype(int)

        # Eventos importantes específicos de Mendoza
        eventos = {
            1: "Vendimia + Verano",
            2: "Otoño tranquilo",
            3: "Invierno + Esquí",
            4: "Primavera + Fiestas"
        }
        
        df_final["evento_importante"] = df_final["trimestre"].map(eventos).fillna("Sin evento específico")
        
        # Variable de temporada turística
        df_final['temporada_alta'] = df_final['trimestre'].isin([1, 3]).astype(int)
        df_final['temporada_vendimia'] = (df_final['trimestre'] == 1).astype(int)

        # Ordenar por indice_tiempo para presentación final
        df_final = df_final.sort_values('indice_tiempo')

        # DEBUG: Verificar contenido final antes de guardar
        logger.info("🔍 VERIFICACIÓN FINAL DEL DATASET CON GOOGLE TRENDS:")
        logger.info(f"   - Total filas: {len(df_final)}")
        logger.info(f"   - Columnas: {list(df_final.columns)}")
        
        # Verificar USD
        if 'precio_promedio_usd' in df_final.columns:
            usd_not_null = df_final['precio_promedio_usd'].notna().sum()
            logger.info(f"   - Datos USD no nulos: {usd_not_null}/{len(df_final)}")
        
        # Verificar Google Trends
        if 'interes_google_promedio' in df_final.columns:
            trends_not_null = df_final['interes_google_promedio'].notna().sum()
            logger.info(f"   - Datos Google Trends no nulos: {trends_not_null}/{len(df_final)}")
            if trends_not_null > 0:
                logger.info(f"   - Rango interés Google: {df_final['interes_google_promedio'].min():.1f} - {df_final['interes_google_promedio'].max():.1f}")
                logger.info(f"   - Muestra Google Trends: {df_final[['indice_tiempo', 'interes_google_promedio']].head()}")

        # Guardar archivo final
        local_data_dir = Path("/usr/local/airflow/data/raw")
        local_data_dir.mkdir(parents=True, exist_ok=True)
        output_path = local_data_dir / "mendoza_turismo_final_con_usd.csv"
        
        df_final.to_csv(output_path, index=False, encoding="utf-8")
        logger.info(f"💾 Dataset guardado en: {output_path}")

        # Verificar que el archivo se guardó correctamente
        if output_path.exists():
            # Leer el archivo guardado para verificar
            df_verificacion = pd.read_csv(output_path)
            logger.info(f"✅ Verificación post-guardado: {len(df_verificacion)} filas, {len(df_verificacion.columns)} columnas")
            
            if 'precio_promedio_usd' in df_verificacion.columns:
                usd_guardado = df_verificacion['precio_promedio_usd'].notna().sum()
                logger.info(f"💰 USD en archivo guardado: {usd_guardado} registros no nulos")
            else:
                logger.error("❌ Columna precio_promedio_usd NO se guardó en el archivo final")
        else:
            logger.error(f"❌ Error: archivo no se guardó en {output_path}")

        # Crear resumen del dataset final - ACTUALIZADO CON GOOGLE TRENDS
        def convert_numpy_types(obj):
            """Convierte tipos numpy a tipos Python nativos para JSON serialization"""
            if isinstance(obj, (pd.Int64Dtype, pd.Float64Dtype)):
                return obj.item()
            elif hasattr(obj, 'item'):
                return obj.item()
            elif isinstance(obj, dict):
                return {k: convert_numpy_types(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_numpy_types(v) for v in obj]
            else:
                return obj

        summary = {
            "creation_timestamp": datetime.now().isoformat(),
            "total_quarters": int(len(df_final)),
            "date_range": f"{df_final['indice_tiempo'].min()} - {df_final['indice_tiempo'].max()}",
            "tourism_data_summary": {
                "total_tourists_avg": int(df_final['turistas_no_residentes_total'].mean()),
                "min_quarter": str(df_final.loc[df_final['turistas_no_residentes_total'].idxmin(), 'indice_tiempo']),
                "max_quarter": str(df_final.loc[df_final['turistas_no_residentes_total'].idxmax(), 'indice_tiempo']),
                "seasonal_pattern": "Q1 y Q3 típicamente más altos (Vendimia y Vacaciones de Invierno)"
            },
            "usd_data_summary": {
                "has_usd_data": usd_quarterly.get("status") == "processed",
                "avg_usd_price": float(df_final['precio_promedio_usd'].mean()) if 'precio_promedio_usd' in df_final and df_final['precio_promedio_usd'].notna().any() else None,
                "usd_quarters_coverage": f"{int(df_final['precio_promedio_usd'].notna().sum())}/{int(len(df_final))}" if 'precio_promedio_usd' in df_final else "0/0",
                "usd_data_found": int(df_final['precio_promedio_usd'].notna().sum()) if 'precio_promedio_usd' in df_final else 0
            },
            "google_trends_summary": {
                "has_trends_data": trends_quarterly.get("status") == "processed",
                "avg_interest": float(df_final['interes_google_promedio'].mean()) if 'interes_google_promedio' in df_final and df_final['interes_google_promedio'].notna().any() else None,
                "trends_quarters_coverage": f"{int(df_final['interes_google_promedio'].notna().sum())}/{int(len(df_final))}" if 'interes_google_promedio' in df_final else "0/0",
                "trends_data_found": int(df_final['interes_google_promedio'].notna().sum()) if 'interes_google_promedio' in df_final else 0
            },
            "features_available": list(df_final.columns),
            "ready_for_modeling": True,
            "recommended_target": "turistas_no_residentes_total",
            "key_predictors": [
                "precio_promedio_usd", "interes_google_promedio", "temporada_alta", 
                "temporada_vendimia", "año", "interes_alto"
            ]
        }

        # Aplicar conversión a todos los valores
        summary = convert_numpy_types(summary)

        summary_path = local_data_dir / "dataset_final_summary.json"
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)

        logger.info("=" * 70)
        logger.info("📊 DATASET FINAL CREADO CON ÉXITO (USD + GOOGLE TRENDS)")
        logger.info("=" * 70)
        logger.info(f"📁 Archivo: {output_path}")
        logger.info(f"📅 Trimestres: {len(df_final)}")
        logger.info(f"🗓️ Rango: {summary['date_range']}")
        logger.info(f"🎯 Variables: {len(df_final.columns)}")
        logger.info(f"💰 Datos USD: {'SÍ' if summary['usd_data_summary']['has_usd_data'] else 'NO'}")
        logger.info(f"📈 Datos Google Trends: {'SÍ' if summary['google_trends_summary']['has_trends_data'] else 'NO'}")
        
        if summary['usd_data_summary']['has_usd_data']:
            if summary['usd_data_summary']['avg_usd_price'] is not None:
                logger.info(f"💵 Precio USD promedio: ${summary['usd_data_summary']['avg_usd_price']:.2f}")
            logger.info(f"📊 Cobertura USD: {summary['usd_data_summary']['usd_quarters_coverage']}")
        
        if summary['google_trends_summary']['has_trends_data']:
            if summary['google_trends_summary']['avg_interest'] is not None:
                logger.info(f"📈 Interés Google promedio: {summary['google_trends_summary']['avg_interest']:.1f}")
            logger.info(f"📊 Cobertura Google Trends: {summary['google_trends_summary']['trends_quarters_coverage']}")
        
        logger.info("✅ LISTO PARA MODELADO PREDICTIVO AVANZADO")
        logger.info("=" * 70)

        return str(output_path)

    except Exception as e:
        logger.error(f"❌ Error creando dataset final con USD y Google Trends: {e}")
        import traceback
        logger.error(f"Traceback completo: {traceback.format_exc()}")
        return ""

    # ─── DAG Definition Mejorado ───────────────────────────────────────────────────

with DAG(
    dag_id="mza_turismo_etl_enhanced",
    default_args=default_args,
    description="Pipeline ETL Mejorado - Predicción Demanda Hotelera Mendoza con USD y Google Trends",
    schedule="@monthly",
    start_date=datetime(2024, 8, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=10,
    tags=["mendoza", "turismo", "economia", "usd", "google-trends", "enhanced", "v2.4"],
    doc_md="""
    ## Pipeline ETL Mejorado - Demanda Hotelera Mendoza v2.4
    
    **NUEVA FUNCIONALIDAD**: Google Trends para interés en búsquedas de "Mendoza"
    
    ### Fuentes de datos principales:
    - **Turismo**: ETI Mendoza (aeropuerto + Cristo Redentor)
    - **USD**: argentinadatos.com (datos diarios históricos oficiales)
    - **Google Trends**: Interés de búsqueda "Mendoza" mensual (agregado trimestral)
    - **Variables temporales**: Estacionales, eventos
    
    ### Salida optimizada:
    - Dataset final con USD y Google Trends sincronizados por trimestre
    - Variables de alto interés de búsqueda y precio
    - Listo para modelos de serie temporal con factores externos
    """,
) as dag:
    # 1. Preparación expandida
    dirs = create_enhanced_directories(ds="{{ ds }}")

    # 2. Descarga de datos tradicionales
    csv_downloads = []
    api_downloads = []
    trends_downloads = []
    
    for spec in DOWNLOAD_SPECS:
        tipo = spec.get("type", "")
        if tipo == "direct_csv":
            download_task = download_direct_csv_enhanced(spec=spec, directories=dirs)
            csv_downloads.append(download_task)
        elif tipo == "api_json":
            api_task = download_api_json(spec=spec, directories=dirs)
            api_downloads.append(api_task)
        elif tipo == "google_trends_csv":
            trends_task = download_google_trends_csv(spec=spec, directories=dirs)
            trends_downloads.append(trends_task)

    # Combinar todas las descargas tradicionales
    all_downloads = csv_downloads + api_downloads + trends_downloads

    # 3. Descarga USD desde argentinadatos.com
    usd_historical = download_usd_historical_dolarapi(directories=dirs)
    
    # 4. Procesar USD a promedios trimestrales
    usd_quarterly = process_usd_to_quarterly_averages(
        usd_data=usd_historical,
        directories=dirs
    )

    # 5. Procesar Google Trends (solo si hay descargas de trends)
    if trends_downloads:
        # Tomar el primer (y único) resultado de Google Trends
        trends_quarterly = process_google_trends_to_quarterly(
            trends_data=trends_downloads[0],
            directories=dirs
        )
    else:
        # Crear tarea dummy que retorna status error
        @task
        def no_trends_available():
            return {"status": "error", "error": "No Google Trends configured"}
        
        trends_quarterly = no_trends_available()

    # 6. Procesamiento tradicional
    processing_result = process_and_standardize_data(
        all_downloads=all_downloads,
        directories=dirs
    )

    # 7. Dataset final MEJORADO con USD y Google Trends sincronizados
    final_dataset_enhanced = create_final_mendoza_dataset(
        processing_summary=processing_result,
        usd_quarterly=usd_quarterly,
        trends_quarterly=trends_quarterly,  # Nuevo parámetro
        directories=dirs
    )

    # 8. Validación de datos
    enhanced_validation = validate_enhanced_data(
        all_downloads=all_downloads,
        processing_summary=processing_result,
        directories=dirs
    )

    # 9. Reporte final
    final_enhanced_report = generate_enhanced_pipeline_report(
        validation_results=enhanced_validation,
        processing_summary=processing_result,
        directories=dirs
    )

    # Dependencias del pipeline - ACTUALIZADAS CON GOOGLE TRENDS
    # Primero los directorios
    dirs >> usd_historical
    
    # Dependencias de descarga
    for download_task in csv_downloads + api_downloads + trends_downloads:
        dirs >> download_task
    
    # USD processing
    usd_historical >> usd_quarterly
    
    # Google Trends processing (solo si hay trends_downloads)
    if trends_downloads:
        trends_downloads[0] >> trends_quarterly
    
    # Processing depende de todas las descargas completadas
    for download_task in csv_downloads + api_downloads + trends_downloads:
        download_task >> processing_result
    
    # Dataset final depende de processing, USD y Trends
    processing_result >> final_dataset_enhanced
    usd_quarterly >> final_dataset_enhanced
    trends_quarterly >> final_dataset_enhanced
    
    # Validation depende de todas las descargas y processing
    for download_task in csv_downloads + api_downloads + trends_downloads:
        download_task >> enhanced_validation
    processing_result >> enhanced_validation
    
    # Reporte final depende de validation y processing
    enhanced_validation >> final_enhanced_report
    processing_result >> final_enhanced_report