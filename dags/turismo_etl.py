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
                    "turismo": validation_results.get("model_readiness", {}).get("has_tourism_data", False),
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
def download_usd_historical_dolarapi(directories: dict) -> dict:
    """
    Descarga datos históricos del dólar oficial desde 
    https://api.argentinadatos.com/v1/dolar/historico
    desde el 01-01-2018 hasta la fecha actual.
    """
    try:
        # Configurar parámetros
        url = "https://api.argentinadatos.com/v1/dolar/historico"
        params = {
            "tipo": "oficial",  # Dólar oficial
            "desde": "2018-01-01",
            "hasta": datetime.now().strftime("%Y-%m-%d")
        }
        
        headers = {
            "User-Agent": "TurismoDataPipeline/2.0",
            "Accept": "application/json"
        }
        
        logger.info(f"🔄 Descargando USD oficial desde {params['desde']} hasta {params['hasta']}")
        logger.info(f"🔗 URL: {url}")
        
        response = requests.get(url, params=params, headers=headers, timeout=120)
        response.raise_for_status()
        
        data = response.json()
        
        if not isinstance(data, list):
            logger.error(f"❌ Formato de respuesta inesperado: {type(data)}")
            return {"status": "error", "error": "Formato de respuesta inesperado"}
        
        if len(data) == 0:
            logger.error("❌ No se recibieron datos USD")
            return {"status": "error", "error": "No se recibieron datos"}
        
        # Validar estructura de datos
        sample_record = data[0]
        logger.info(f"📋 Estructura de datos de muestra: {list(sample_record.keys())}")
        
        # Guardar datos raw
        raw_dir = Path(directories["raw"]) / "economico"
        raw_dir.mkdir(parents=True, exist_ok=True)
        dest_path = raw_dir / "usd_historico_argentinadatos.json"
        
        with open(dest_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        # Análisis de datos recibidos
        dates = [record.get('fecha') for record in data if record.get('fecha')]
        date_range = f"{min(dates)} - {max(dates)}" if dates else "N/A"
        
        logger.info(f"✅ Datos USD descargados desde argentinadatos.com: {len(data)} registros")
        logger.info(f"📊 Rango de fechas: {date_range}")
        logger.info(f"📋 Campos en cada registro: {list(sample_record.keys())}")
        
        return {
            "status": "downloaded",
            "path": str(dest_path),
            "records": len(data),
            "data": data,
            "date_range": date_range,
            "api_source": "argentinadatos.com"
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
        
        # Agregación trimestral
        df_quarterly = df.groupby("anio_trimestre").agg(
            precio_promedio_usd=("venta", "mean"),
            volatilidad_usd=("venta", "std"),
            precio_min_usd=("venta", "min"),
            precio_max_usd=("venta", "max"),
            dias=("venta", "count")
        ).reset_index()
        
        # Renombrar columna para merge con turismo
        df_quarterly = df_quarterly.rename(columns={"anio_trimestre": "indice_tiempo"})
        
        # Redondear valores
        df_quarterly["precio_promedio_usd"] = df_quarterly["precio_promedio_usd"].round(2)
        df_quarterly["volatilidad_usd"] = df_quarterly["volatilidad_usd"].round(2)
        df_quarterly["precio_min_usd"] = df_quarterly["precio_min_usd"].round(2)
        df_quarterly["precio_max_usd"] = df_quarterly["precio_max_usd"].round(2)
        
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
def create_final_mendoza_dataset(
    processing_summary: Dict[str, Any],
    usd_quarterly: Dict[str, Any],
    directories: Dict[str, str]
) -> str:
    """Crea dataset final trimestral con USD sincronizado."""
    try:
        logger.info("🎯 Creando dataset final con USD desde argentinadatos.com...")

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
        
        df_aeropuerto = df_aeropuerto_full[["indice_tiempo", "turistas_no_residentes"]].copy()
        df_cristo = df_cristo_full[["indice_tiempo", "turistas_no_residentes"]].copy()

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

        # Preparar dataset final con datos de turismo
        df_final = turistas_agg[["indice_tiempo", "turistas_no_residentes_total", "turistas_aeropuerto", "turistas_cristo"]].copy()

        # Merge con datos USD procesados desde argentinadatos.com
        if usd_quarterly.get("status") == "processed":
            logger.info("💰 Mergeando con datos USD trimestrales desde argentinadatos.com...")
            
            # Leer datos USD trimestrales
            df_usd = pd.read_csv(usd_quarterly["simple_path"])
            
            logger.info(f"📊 Datos USD disponibles: {len(df_usd)} trimestres")
            logger.info(f"📊 Rango USD: {usd_quarterly.get('date_range', 'N/A')}")
            
            # Merge con datos USD
            df_final = df_final.merge(df_usd, on="indice_tiempo", how="left")
            
            # Verificar merge
            usd_matches = df_final['precio_promedio_usd'].notna().sum()
            logger.info(f"✅ Merge USD completado: {usd_matches}/{len(df_final)} trimestres con datos USD")
            
            # Crear variables derivadas del USD
            if usd_matches > 0:
                df_final['usd_alto'] = (df_final['precio_promedio_usd'] > df_final['precio_promedio_usd'].median()).astype(int)
                df_final['usd_muy_volatil'] = (df_final['volatilidad_usd'] > 5).astype(int)
            
        else:
            logger.warning("⚠️ No hay datos USD procesados disponibles")
            df_final["precio_promedio_usd"] = None
            df_final["volatilidad_usd"] = None

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
        
        # Crear variables lag para detectar tendencias
        df_final = df_final.sort_values(['año', 'trimestre'])
        df_final['turistas_lag1'] = df_final['turistas_no_residentes_total'].shift(1)
        df_final['turistas_variacion'] = df_final['turistas_no_residentes_total'] - df_final['turistas_lag1']
        df_final['turistas_crecimiento'] = (df_final['turistas_variacion'] / df_final['turistas_lag1'] * 100).round(2)

        # Ordenar por indice_tiempo para presentación final
        df_final = df_final.sort_values('indice_tiempo')

        # Guardar archivo final
        local_data_dir = Path("/usr/local/airflow/data/raw")
        local_data_dir.mkdir(parents=True, exist_ok=True)
        output_path = local_data_dir / "mendoza_turismo_final_con_usd.csv"
        df_final.to_csv(output_path, index=False, encoding="utf-8")

        # Crear resumen del dataset final
        summary = {
            "creation_timestamp": datetime.now().isoformat(),
            "total_quarters": len(df_final),
            "date_range": f"{df_final['indice_tiempo'].min()} - {df_final['indice_tiempo'].max()}",
            "tourism_data_summary": {
                "total_tourists_avg": round(df_final['turistas_no_residentes_total'].mean()),
                "min_quarter": df_final.loc[df_final['turistas_no_residentes_total'].idxmin(), 'indice_tiempo'],
                "max_quarter": df_final.loc[df_final['turistas_no_residentes_total'].idxmax(), 'indice_tiempo'],
                "seasonal_pattern": "Q1 y Q3 típicamente más altos (Vendimia y Vacaciones de Invierno)"
            },
            "usd_data_summary": {
                "has_usd_data": usd_quarterly.get("status") == "processed",
                "avg_usd_price": round(df_final['precio_promedio_usd'].mean(), 2) if 'precio_promedio_usd' in df_final and df_final['precio_promedio_usd'].notna().any() else None,
                "usd_quarters_coverage": f"{df_final['precio_promedio_usd'].notna().sum()}/{len(df_final)}" if 'precio_promedio_usd' in df_final else "0/0"
            },
            "features_available": list(df_final.columns),
            "ready_for_modeling": True,
            "recommended_target": "turistas_no_residentes_total",
            "key_predictors": [
                "precio_promedio_usd", "volatilidad_usd", "temporada_alta", 
                "temporada_vendimia", "turistas_lag1", "año"
            ]
        }

        summary_path = local_data_dir / "dataset_final_summary.json"
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)

        logger.info("=" * 70)
        logger.info("📊 DATASET FINAL CREADO CON ÉXITO")
        logger.info("=" * 70)
        logger.info(f"📁 Archivo: {output_path}")
        logger.info(f"📅 Trimestres: {len(df_final)}")
        logger.info(f"🗓️ Rango: {summary['date_range']}")
        logger.info(f"🎯 Variables: {len(df_final.columns)}")
        logger.info(f"💰 Datos USD: {'SÍ' if summary['usd_data_summary']['has_usd_data'] else 'NO'}")
        
        if summary['usd_data_summary']['has_usd_data']:
            logger.info(f"💵 Precio USD promedio: ${summary['usd_data_summary']['avg_usd_price']}")
            logger.info(f"📊 Cobertura USD: {summary['usd_data_summary']['usd_quarters_coverage']}")
        
        logger.info("✅ LISTO PARA MODELADO PREDICTIVO")
        logger.info("=" * 70)

        return str(output_path)

    except Exception as e:
        logger.error(f"❌ Error creando dataset final con USD: {e}")
        return ""

# ─── DAG Definition Mejorado ───────────────────────────────────────────────────

with DAG(
    dag_id="mza_turismo_etl_enhanced",
    default_args=default_args,
    description="Pipeline ETL Mejorado - Predicción Demanda Hotelera Mendoza con USD desde argentinadatos.com",
    schedule="@monthly",
    start_date=datetime(2024, 8, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=10,
    tags=["mendoza", "turismo", "economia", "usd", "argentinadatos", "enhanced", "v2.3"],
    doc_md="""
    ## Pipeline ETL Mejorado - Demanda Hotelera Mendoza v2.3
    
    **FUNCIONALIDAD**: Datos USD históricos desde argentinadatos.com API
    
    ### Fuentes de datos principales:
    - **Turismo**: ETI Mendoza (aeropuerto + Cristo Redentor)
    - **USD**: argentinadatos.com (datos diarios históricos oficiales)
    - **Variables temporales**: Estacionales, eventos, tendencias
    
    ### Salida optimizada:
    - Dataset final con USD sincronizado por trimestre
    - Variables lag y de crecimiento
    - Listo para modelos predictivos avanzados
    """,
) as dag:
    # 1. Preparación expandida
    dirs = create_enhanced_directories(ds="{{ ds }}")

    # 2. Descarga de datos tradicionales
    csv_downloads = []
    api_downloads = []
    
    for spec in DOWNLOAD_SPECS:
        tipo = spec.get("type", "")
        if tipo == "direct_csv":
            download_task = download_direct_csv_enhanced(spec=spec, directories=dirs)
            csv_downloads.append(download_task)
        elif tipo == "api_json":
            api_task = download_api_json(spec=spec, directories=dirs)
            api_downloads.append(api_task)

    # Combinar todas las descargas
    all_downloads = csv_downloads + api_downloads

    # 3. Descarga USD desde argentinadatos.com
    usd_historical = download_usd_historical_dolarapi(directories=dirs)
    
    # 4. Procesar USD a promedios trimestrales
    usd_quarterly = process_usd_to_quarterly_averages(
        usd_data=usd_historical,
        directories=dirs
    )

    # 5. Procesamiento tradicional
    processing_result = process_and_standardize_data(
        all_downloads=all_downloads,
        directories=dirs
    )

    # 6. Dataset final MEJORADO con USD sincronizado
   
    final_dataset_with_usd = create_final_mendoza_dataset(
        processing_summary=processing_result,
        usd_quarterly=usd_quarterly,
        directories=dirs
    )

    # 7. Matriz de features mensuales
    features_matrix = create_monthly_features_matrix(
        processing_summary=processing_result,
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
        features_path=features_matrix,
        directories=dirs
    )

    # Dependencias del pipeline - CORREGIDAS
    # Primero los directorios
    dirs >> usd_historical
    
    # Dependencias de descarga de CSV y API
    for download_task in csv_downloads:
        dirs >> download_task
    for download_task in api_downloads:
        dirs >> download_task
    
    # USD processing
    usd_historical >> usd_quarterly
    
    # Processing depende de todas las descargas completadas
    for download_task in csv_downloads + api_downloads:
        download_task >> processing_result
    
    # Dataset final depende de processing y USD
    processing_result >> final_dataset_with_usd
    usd_quarterly >> final_dataset_with_usd
    
    # Features depende de processing
    processing_result >> features_matrix
    
    # Validation depende de todas las descargas y processing
    for download_task in csv_downloads + api_downloads:
        download_task >> enhanced_validation
    processing_result >> enhanced_validation
    
    # Reporte final depende de validation, processing y features
    enhanced_validation >> final_enhanced_report
    processing_result >> final_enhanced_report
    features_matrix >> final_enhanced_report