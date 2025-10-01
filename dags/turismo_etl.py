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
    """Construye especificaciones expandidas incluyendo TODAS las nuevas fuentes ETI."""
    specs = []
    defaults = cfg.get("defaults", {})
    
    # 1. TODAS las fuentes turísticas ETI
    eti_sources = [
        "eti_aeropuerto",
        "eti_cristo_redentor", 
        "eti_ezeiza_aeroparque",
        "eti_cordoba_aeropuerto",
        "eti_puerto_buenos_aires"
    ]
    
    for eti_source in eti_sources:
        eti_config = cfg.get(eti_source, {})
        if eti_config and eti_config.get("enabled", True):
            api_url = eti_config.get("api_url")
            if api_url:
                specs.append({
                    "src": eti_source,
                    "name": f"{eti_config.get('dataset_name', eti_source)}.csv",
                    "url": api_url,
                    "type": "direct_csv",
                    "min_bytes": eti_config.get("min_bytes", 2000),
                    "description": f"ETI - {eti_source.replace('_', ' ').title()}",
                    "category": "turismo",
                    "frequency": eti_config.get("frequency", "trimestral")  # Importante para división
                })
                logger.info(f"✅ ETI {eti_source} spec configurado - {eti_config.get('frequency', 'trimestral')}")
    
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
        trends_url = f"https://trends.google.es/trends/explore?date=2014-01-01%20{fecha_actual}&geo=AR&q=Mendoza&hl=es"
        
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
            "date_from": google_trends_config.get("date_from", "2014-01-01"),
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
    """Descarga CSV con categorización y validación mejorada - INCLUYE FRECUENCIA."""
    try:
        src = spec["src"]
        name = spec["name"] 
        url = spec["url"]
        min_bytes = spec["min_bytes"]
        category = spec.get("category", "general")
        frequency = spec.get("frequency", "unknown")  # NUEVA: capturar frecuencia
        
        raw_dir = Path(directories["raw"]) / category
        dest_path = raw_dir / name
        
        logger.info(f"📥 Descargando {spec['description']}: {name} (Frecuencia: {frequency})")
        
        if dest_path.exists() and dest_path.stat().st_size >= min_bytes:
            size = dest_path.stat().st_size
            logger.info(f"✅ Archivo existente válido: {size:,} bytes")
            return {
                "src": src, "name": name, "path": str(dest_path),
                "size": size, "status": "cached", "url": url,
                "description": spec["description"], "category": category,
                "frequency": frequency  # NUEVA: incluir frecuencia en resultado
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
        
        logger.info(f"✅ Descarga exitosa: {name} - {total_size:,} bytes - Frecuencia: {frequency}")
        
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
            "category": spec.get("category", "unknown"),
            "frequency": spec.get("frequency", "unknown")
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
    """Procesa y estandariza todos los datos descargados - MODIFICADO para agrupar por PAÍS DE ORIGEN."""
    try:
        files = []
        for download in all_downloads:
            if isinstance(download, list):
                files.extend(download)
            else:
                files.append(download)
        
        logger.info(f"📥 Total archivos descargados: {len(files)}")
        for file_info in files:
            logger.info(f"  📄 {file_info.get('name', 'unknown')}: {file_info.get('status', 'unknown')} - {file_info.get('category', 'unknown')} - Freq: {file_info.get('frequency', 'N/A')}")
        
        processed_files = {
            "turismo": [],
            "economico": [], 
            "infraestructura": [],
            "general": [],
            "trends": []
        }
        
        processed_dir = Path(directories["processed"])
        
        def expand_quarterly_to_monthly_by_country(df, fecha_col, turistas_col, pais_col, file_name):
            """
            Expande datos trimestrales a mensuales por país de origen, distribuyendo el valor 
            trimestral en los 3 meses correspondientes - CON TRUNCAMIENTO A ENTEROS POR PAÍS.
            """
            logger.info(f"🔄 Expandiendo datos trimestrales a mensuales POR PAÍS para {file_name}")
            
            expanded_rows = []
            
            for _, row in df.iterrows():
                fecha_trimestre = row[fecha_col]
                turistas_original = row[turistas_col]
                pais_origen = row[pais_col]
                
                # TRUNCAR a entero después de dividir por 3 (sin decimales) POR PAÍS
                turistas_por_mes = int(turistas_original / 3)
                
                logger.debug(f"📊 {file_name} - {pais_origen}: Trimestre {turistas_original} -> {turistas_por_mes} turistas/mes (truncado)")
                
                # Obtener año y trimestre de la fecha
                año = fecha_trimestre.year
                mes_inicio = fecha_trimestre.month
                
                # Determinar los 3 meses del trimestre
                if mes_inicio in [1, 2, 3]:  # Q1
                    meses = [1, 2, 3]
                elif mes_inicio in [4, 5, 6]:  # Q2
                    meses = [4, 5, 6]
                elif mes_inicio in [7, 8, 9]:  # Q3
                    meses = [7, 8, 9]
                else:  # Q4
                    meses = [10, 11, 12]
                
                # Crear una fila para cada mes del trimestre MANTENIENDO EL PAÍS
                for mes in meses:
                    fecha_mensual = pd.Timestamp(year=año, month=mes, day=1)
                    indice_mensual = fecha_mensual.strftime('%Y-%m')
                    
                    expanded_rows.append({
                        fecha_col: fecha_mensual,
                        turistas_col: turistas_por_mes,
                        pais_col: pais_origen,  # MANTENER PAÍS DE ORIGEN
                        'indice_tiempo': indice_mensual,
                        'fecha_std': fecha_mensual
                    })
                    
                    logger.debug(f"📅 {file_name} - {pais_origen}: {fecha_trimestre.strftime('%Y-%m')} -> {indice_mensual}: {turistas_por_mes} turistas")
            
            df_expanded = pd.DataFrame(expanded_rows)
            
            # Verificar totales por país
            logger.info(f"✅ {file_name}: Expandido de {len(df)} trimestres a {len(df_expanded)} registros mensuales por país")
            
            # Log por país
            paises_unicos = df_expanded[pais_col].unique()
            for pais in paises_unicos[:5]:  # Mostrar solo los primeros 5 países
                total_pais = df_expanded[df_expanded[pais_col] == pais][turistas_col].sum()
                logger.info(f"📊 {file_name} - {pais}: {total_pais} turistas totales (expandido)")
            
            df_expanded[turistas_col] = df_expanded[turistas_col].astype(int)
            return df_expanded
        
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
                    
                    # Procesar archivos ETI - NUEVA LÓGICA CON PAÍSES DE ORIGEN
                    if category == "turismo" and any(eti in file_info.get("src", "").lower() for eti in ["eti_", "aeropuerto", "cristo", "ezeiza", "cordoba", "puerto"]):
                        logger.info(f"📊 Procesando ETI {file_info['name']}: {len(df)} filas, columnas: {list(df.columns)}")
                        
                        frequency = file_info.get("frequency", "unknown")
                        es_trimestral = frequency == "trimestral" or "trimes" in file_info.get("name", "").lower()
                        
                        logger.info(f"📅 ETI {file_info['name']} - Frecuencia: {frequency} - Es trimestral: {es_trimestral}")
                        
                        # Buscar columnas necesarias DE MANERA MÁS FLEXIBLE
                        fecha_col = None
                        turistas_col = None
                        pais_col = None  # NUEVA: columna de país
                        
                        # Buscar columna de fecha
                        for col in df.columns:
                            col_lower = col.lower()
                            if any(date_keyword in col_lower for date_keyword in [
                                'fecha', 'periodo', 'anio_trimestre', 'trimestre', 'año_trimestre', 
                                'indice_tiempo_periodo', 'indice_tiempo', 'time', 'date'
                            ]):
                                fecha_col = col
                                break
                        
                        # Buscar columna de turistas
                        for col in df.columns:
                            col_lower = col.lower()
                            if any(turistas_keyword in col_lower for turistas_keyword in [
                                'turistas_no_residentes', 'turistas_extranjeros', 'turistas',
                                'visitantes_no_residentes', 'no_residentes', 'extranjeros', 'visitors'
                            ]):
                                turistas_col = col
                                break
                        
                        # NUEVA: Buscar columna de país/residencia de origen
                        for col in df.columns:
                            col_lower = col.lower()
                            if any(pais_keyword in col_lower for pais_keyword in [
                                'residencia', 'pais', 'country', 'origin', 'nacionalidad', 
                                'procedencia', 'pais_origen', 'lugar_residencia', 'pais_de_residencia'
                            ]):
                                pais_col = col
                                break
                        
                        if fecha_col and turistas_col and pais_col:
                            logger.info(f"✅ ETI {file_info['name']} - Columnas encontradas: fecha='{fecha_col}', turistas='{turistas_col}', país='{pais_col}'")
                            
                            # Verificar países únicos disponibles
                            paises_unicos = df[pais_col].unique()
                            logger.info(f"🌍 ETI {file_info['name']} - Países únicos encontrados: {len(paises_unicos)}")
                            logger.info(f"🌍 Primeros países: {list(paises_unicos)[:10]}")
                            
                            # Limpiar y normalizar nombres de países
                            df[pais_col] = df[pais_col].astype(str).str.strip().str.title()
                            
                            # Convertir turistas a numérico ANTES de cualquier procesamiento
                            df[turistas_col] = pd.to_numeric(df[turistas_col], errors='coerce').fillna(0).astype(int)
                            
                            logger.info(f"📊 ETI {file_info['name']} - Total registros por país antes de filtros: {len(df)}")
                            
                            try:
                                # Convertir fechas
                                df[fecha_col] = pd.to_datetime(df[fecha_col], errors='coerce')
                                df = df[df[fecha_col].notna()]
                                df = df[df[fecha_col] >= '2014-01-01']
                                
                                logger.info(f"✅ ETI {file_info['name']} - Datos después de filtro fecha: {len(df)} registros válidos")
                                
                                # EXPANSIÓN TRIMESTRAL A MENSUAL POR PAÍS
                                if es_trimestral:
                                    logger.info(f"📊 ETI {file_info['name']} - EXPANDIENDO DATOS TRIMESTRALES A MENSUALES POR PAÍS")
                                    df = expand_quarterly_to_monthly_by_country(df, fecha_col, turistas_col, pais_col, file_info['name'])
                                else:
                                    logger.info(f"📊 ETI {file_info['name']} - Datos mensuales, creando índice temporal")
                                    df[turistas_col] = df[turistas_col].astype(int)
                                    df['indice_tiempo'] = df[fecha_col].dt.strftime('%Y-%m')
                                    df['fecha_std'] = df[fecha_col]
                                
                                # Asegurar que todos los turistas sean enteros
                                df[turistas_col] = df[turistas_col].astype(int)
                                
                                logger.info(f"✅ ETI {file_info['name']} - Procesamiento completado: {len(df)} registros mensuales por país")
                                logger.info(f"📊 ETI {file_info['name']} - Total turistas por país (muestra):")
                                
                                # Mostrar resumen por país
                                resumen_paises = df.groupby(pais_col)[turistas_col].sum().sort_values(ascending=False)
                                for pais, total in resumen_paises.head(5).items():
                                    logger.info(f"  🌍 {pais}: {total} turistas totales")
                                
                                logger.info(f"📅 ETI {file_info['name']} - Rango temporal: {df['indice_tiempo'].min()} - {df['indice_tiempo'].max()}")
                                
                            except Exception as e:
                                logger.error(f"❌ ETI {file_info['name']} - Error procesando fechas: {e}")
                                continue
                        else:
                            logger.error(f"❌ ETI {file_info['name']} - Columnas faltantes. Fecha: {fecha_col}, Turistas: {turistas_col}, País: {pais_col}")
                            logger.error(f"📋 Columnas disponibles: {list(df.columns)}")
                            continue
                
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
                
                # Filtrar datos desde 2014 en adelante
                if 'fecha_std' in df.columns:
                    original_rows = len(df)
                    valid_dates = df['fecha_std'].notna().sum()
                    logger.info(f"📅 {file_info['name']} - Fechas válidas: {valid_dates}/{original_rows}")
                    
                    if valid_dates > 0:
                        df = df[df['fecha_std'] >= '2014-01-01']
                        filtered_rows = len(df)
                        if original_rows != filtered_rows:
                            logger.info(f"📅 Filtro 2014+: {original_rows} -> {filtered_rows} registros en {file_info['name']}")
                
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
                    "has_country_column": 'residencia' in df.columns or any('pais' in col.lower() for col in df.columns),  # NUEVA
                    "date_range": f"{df['fecha_std'].min()} - {df['fecha_std'].max()}" if 'fecha_std' in df.columns else "N/A",
                    "original_status": file_info.get("status", "unknown"),
                    "data_source": file_info.get("src", "unknown"),
                    "frequency": file_info.get("frequency", "unknown"),
                    "was_quarterly_expanded": file_info.get("frequency") == "trimestral" and category == "turismo",
                    "integers_enforced": category == "turismo",
                    "country_processed": category == "turismo"  # NUEVA: indicar procesamiento por país
                })
                
                logger.info(f"✅ Procesado {category}/{file_info['name']}: {len(df)} filas, {len(df.columns)} columnas - Por país: {category == 'turismo'}")
                
            except Exception as e:
                logger.error(f"❌ Error procesando {file_info['name']}: {e}")
                import traceback
                logger.error(f"Traceback: {traceback.format_exc()}")
                continue
        
        # Resumen mejorado con información de países
        summary = {
            "timestamp": datetime.now().isoformat(),
            "files_by_category": {cat: len(files) for cat, files in processed_files.items()},
            "total_processed": sum(len(files) for files in processed_files.values()),
            "processed_files": processed_files,
            "success": True,
            "quarterly_files_expanded": sum(1 for cat_files in processed_files.values() 
                                          for file_info in cat_files 
                                          if file_info.get("was_quarterly_expanded", False)),
            "integer_enforcement_applied": sum(1 for cat_files in processed_files.values()
                                             for file_info in cat_files
                                             if file_info.get("integers_enforced", False)),
            "country_processing_applied": sum(1 for cat_files in processed_files.values()  # NUEVA
                                            for file_info in cat_files
                                            if file_info.get("country_processed", False))
        }
        
        # Guardar resumen
        summary_path = processed_dir / "processing_summary.json"
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        
        logger.info(f"📊 Procesamiento completado: {summary['total_processed']} archivos")
        logger.info(f"📊 Archivos trimestrales expandidos a mensuales: {summary['quarterly_files_expanded']}")
        logger.info(f"📊 Archivos procesados por país: {summary['country_processing_applied']}")  # NUEVA
        
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
                "general": {"files": 0, "status": "unknown"},
                "trends": {"files": 0, "status": "unknown"}  # AGREGAR CATEGORÍA TRENDS
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
    y filtra por dólar oficial desde 2014-01-01 hasta la fecha actual.
    """
    try:
        # Nueva URL que devuelve todo el histórico
        url = "https://api.argentinadatos.com/v1/cotizaciones/dolares/"
        
        headers = {
            "User-Agent": "TurismoDataPipeline/3.0",
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
        
        # Filtrar por dólar oficial y fecha desde 2014-01-01 (CAMBIO)
        fecha_inicio = datetime(2014, 1, 1)  # NUEVA FECHA INICIO
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
                
                # Filtrar por rango de fechas desde 2014
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
            logger.error("❌ No se encontraron datos del dólar oficial desde 2014-01-01")
            return {"status": "error", "error": "No hay datos del dólar oficial en el rango de fechas"}
        
        logger.info(f"✅ Datos filtrados del dólar oficial: {len(datos_filtrados)} registros desde 2014")
        
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
        
        return {
            "status": "downloaded",
            "path": str(dest_path),
            "records": len(datos_filtrados),
            "data": datos_filtrados,
            "date_range": date_range,
            "api_source": "argentinadatos.com",
            "filter_applied": "dolar_oficial_desde_2014"
        }
        
    except Exception as e:
        logger.error(f"❌ Error descargando USD desde argentinadatos.com: {e}")
        return {"status": "error", "error": str(e)}

@task
def process_usd_to_monthly_averages(
    usd_data: dict,
    directories: dict
) -> dict:
    """
    Procesa los datos históricos del dólar para obtener promedios, mínimos y máximos MENSUALES.
    Adaptado para datos mensuales desde 2014 con variación mensual.
    """
    try:
        if usd_data.get("status") != "downloaded":
            logger.error("No se puede procesar datos USD: descarga fallida")
            return {"status": "error"}
        
        data = usd_data["data"]
        df = pd.DataFrame(data)
        
        logger.info(f"📊 Procesando {len(df)} registros de USD MENSUAL desde {usd_data.get('api_source', 'API')}")
        
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
        df = df[df["fecha"] >= "2014-01-01"]
        
        # Asegurar que venta sea numérico
        df["venta"] = pd.to_numeric(df[venta_col], errors="coerce")
        df = df[df["venta"].notna()]
        
        logger.info(f"📅 Datos después de limpieza: {len(df)} registros")
        logger.info(f"📊 Rango USD: ${df['venta'].min():.2f} - ${df['venta'].max():.2f}")
        
        # Crear índice MENSUAL - FORMATO YYYY-MM
        df["año_mes"] = df["fecha"].dt.strftime('%Y-%m')
        
        # Agregación MENSUAL - AMPLIADA con min, max y variación
        df_monthly = df.groupby("año_mes").agg(
            precio_promedio_usd=("venta", "mean"),
            precio_minimo_usd=("venta", "min"),      # NUEVO: valor mínimo del mes
            precio_maximo_usd=("venta", "max"),      # NUEVO: valor máximo del mes
            dias=("venta", "count")
        ).reset_index()
        
        # CALCULAR VARIACIÓN MENSUAL (diferencia entre máximo y mínimo)
        df_monthly["variacion_usd_mensual"] = df_monthly["precio_maximo_usd"] - df_monthly["precio_minimo_usd"]
        
        # CALCULAR PORCENTAJE DE VARIACIÓN MENSUAL
        df_monthly["variacion_porcentual_usd"] = (
            (df_monthly["precio_maximo_usd"] - df_monthly["precio_minimo_usd"]) / 
            df_monthly["precio_minimo_usd"] * 100
        ).round(2)
        
        # Renombrar columna para merge con turismo
        df_monthly = df_monthly.rename(columns={"año_mes": "indice_tiempo"})
        
        # Redondear valores
        df_monthly["precio_promedio_usd"] = df_monthly["precio_promedio_usd"].round(2)
        df_monthly["precio_minimo_usd"] = df_monthly["precio_minimo_usd"].round(2)
        df_monthly["precio_maximo_usd"] = df_monthly["precio_maximo_usd"].round(2)
        df_monthly["variacion_usd_mensual"] = df_monthly["variacion_usd_mensual"].round(2)
        
        # Log estadísticas de variación
        logger.info(f"💰 Estadísticas de variación USD mensual:")
        logger.info(f"  📊 Variación promedio: ${df_monthly['variacion_usd_mensual'].mean():.2f}")
        logger.info(f"  📊 Variación máxima: ${df_monthly['variacion_usd_mensual'].max():.2f}")
        logger.info(f"  📊 Variación mínima: ${df_monthly['variacion_usd_mensual'].min():.2f}")
        logger.info(f"  📊 Variación porcentual promedio: {df_monthly['variacion_porcentual_usd'].mean():.2f}%")
        
        # Guardar CSV procesado MENSUAL
        processed_dir = Path(directories["processed"]) / "economico"
        processed_dir.mkdir(parents=True, exist_ok=True)
        monthly_path = processed_dir / "usd_monthly_argentinadatos.csv"
        df_monthly.to_csv(monthly_path, index=False, encoding="utf-8")
        
        logger.info(f"✅ USD mensual procesado: {len(df_monthly)} meses")
        logger.info(f"📊 Rango temporal: {df_monthly['indice_tiempo'].min()} - {df_monthly['indice_tiempo'].max()}")
        logger.info(f"💰 Precio promedio general: ${df_monthly['precio_promedio_usd'].mean():.2f}")
        logger.info(f"📋 Columnas USD generadas: {list(df_monthly.columns)}")
        
        return {
            "status": "processed",
            "monthly_path": str(monthly_path),
            "records": len(df_monthly),
            "date_range": f"{df_monthly['indice_tiempo'].min()} - {df_monthly['indice_tiempo'].max()}",
            "avg_usd_price": round(df_monthly['precio_promedio_usd'].mean(), 2),
            "avg_variation": round(df_monthly['variacion_usd_mensual'].mean(), 2),
            "max_variation": round(df_monthly['variacion_usd_mensual'].max(), 2)
        }
    except Exception as e:
        logger.error(f"❌ Error procesando USD mensual: {e}")
        return {"status": "error", "error": str(e)}

@task(execution_timeout=timedelta(minutes=15))
def download_google_trends_csv(
    spec: Dict[str, Any],
    directories: Dict[str, str]
) -> Dict[str, Any]:
    """Descarga datos de Google Trends para el término 'Mendoza' desde 2014 - MENSUAL."""
    try:
        src = spec["src"]
        name = spec["name"] 
        search_term = spec.get("search_term", "Mendoza")
        geo = spec.get("geo", "AR")
        date_from = spec.get("date_from", "2014-01-01")  # NUEVA FECHA INICIO
        date_to = spec.get("date_to", datetime.now().strftime('%Y-%m-%d'))
        category = spec.get("category", "trends")
        
        raw_dir = Path(directories["raw"]) / category
        raw_dir.mkdir(parents=True, exist_ok=True)
        dest_path = raw_dir / name
        
        logger.info(f"📈 Descargando Google Trends MENSUAL para '{search_term}' desde {date_from} hasta {date_to}")
        
        try:
            # Importar pytrends si está disponible
            from pytrends.request import TrendReq
            
            # Configurar pytrends
            pytrends = TrendReq(hl='es', tz=360)
            
            # Construir timeframe para pytrends (formato: YYYY-MM-DD YYYY-MM-DD)
            timeframe = f"{date_from} {date_to}"
            
            # Realizar búsqueda
            logger.info(f"🔍 Buscando tendencia MENSUAL para: {search_term} en {geo} durante {timeframe}")
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
            
            logger.info(f"✅ Google Trends MENSUAL descargado: {len(interest_over_time_df)} registros mensuales")
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
def process_google_trends_to_monthly(
    trends_data: dict,
    directories: dict
) -> dict:
    """
    Procesa datos mensuales de Google Trends directamente (ya son mensuales).
    """
    try:
        if trends_data.get("status") != "downloaded":
            logger.error("No se puede procesar Google Trends: descarga fallida")
            return {"status": "error", "error": "Trends download failed"}
        
        trends_path = trends_data["path"]
        
        # Verificar que el archivo existe
        if not Path(trends_path).exists():
            logger.error(f"Archivo de Google Trends no existe: {trends_path}")
            return {"status": "error", "error": "Trends file not found"}
        
        # Leer datos de Google Trends
        df_trends = pd.read_csv(trends_path)
        
        logger.info(f"📈 Procesando Google Trends MENSUAL: {len(df_trends)} registros mensuales")
        logger.info(f"📋 Columnas Google Trends: {list(df_trends.columns)}")
        
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
        df_trends = df_trends[df_trends["fecha"] >= "2014-01-01"]  # FILTRO DESDE 2014
        
        # Asegurar que el interés sea numérico
        df_trends["interes_google"] = pd.to_numeric(df_trends[interes_col], errors="coerce")
        df_trends = df_trends[df_trends["interes_google"].notna()]
        
        logger.info(f"📅 Datos después de limpieza: {len(df_trends)} registros")
        logger.info(f"📊 Rango de interés: {df_trends['interes_google'].min()} - {df_trends['interes_google'].max()}")
        
        # Crear índice MENSUAL - FORMATO YYYY-MM
        df_trends["año_mes"] = df_trends["fecha"].dt.strftime('%Y-%m')
        
        # Como los datos ya son mensuales, solo agregamos por mes si hay duplicados
        df_monthly = df_trends.groupby("año_mes").agg(
            interes_google_promedio=("interes_google", "mean"),
            interes_google_max=("interes_google", "max"),
            interes_google_min=("interes_google", "min"),
            registros=("interes_google", "count")
        ).reset_index()
        
        # Renombrar columna para merge
        df_monthly = df_monthly.rename(columns={"año_mes": "indice_tiempo"})
        
        # Redondear valores
        df_monthly["interes_google_promedio"] = df_monthly["interes_google_promedio"].round(1)
        
        # CREAR VARIABLE DE ALTO INTERÉS (por encima de la mediana) - CORREGIDO
        mediana_interes = df_monthly["interes_google_promedio"].median()
        df_monthly["interes_alto"] = (df_monthly["interes_google_promedio"] > mediana_interes).astype(int)
        
        logger.info(f"📈 Variable interes_alto creada - Mediana: {mediana_interes:.1f}")
        logger.info(f"📊 Distribución interes_alto: {df_monthly['interes_alto'].value_counts().to_dict()}")
        
        # Guardar CSV procesado MENSUAL
        processed_dir = Path(directories["processed"]) / "trends"
        processed_dir.mkdir(parents=True, exist_ok=True)
        trends_monthly_path = processed_dir / "google_trends_mendoza_monthly.csv"
        df_monthly.to_csv(trends_monthly_path, index=False, encoding="utf-8")
        
        logger.info(f"✅ Google Trends mensual procesado: {len(df_monthly)} meses")
        logger.info(f"📊 Rango temporal: {df_monthly['indice_tiempo'].min()} - {df_monthly['indice_tiempo'].max()}")
        logger.info(f"📈 Interés promedio general: {df_monthly['interes_google_promedio'].mean():.1f}")
        logger.info(f"📋 Columnas finales: {list(df_monthly.columns)}")
        
        return {
            "status": "processed",
            "monthly_path": str(trends_monthly_path),
            "records": len(df_monthly),
            "date_range": f"{df_monthly['indice_tiempo'].min()} - {df_monthly['indice_tiempo'].max()}",
            "avg_interest": round(df_monthly['interes_google_promedio'].mean(), 1),
            "median_interest": round(mediana_interes, 1)
        }
        
    except Exception as e:
        logger.error(f"❌ Error procesando Google Trends mensual: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return {"status": "error", "error": str(e)}

@task
def create_final_monthly_dataset(
    processing_summary: Dict[str, Any],
    usd_monthly: Dict[str, Any],
    trends_monthly: Dict[str, Any],
    directories: Dict[str, str]
) -> str:
    """Crea dataset final MENSUAL agrupado POR PAÍS DE ORIGEN."""
    try:
        logger.info("🎯 Creando dataset final MENSUAL AGRUPADO POR PAÍS DE ORIGEN...")

        # Función auxiliar para convertir fecha - MOVER AL PRINCIPIO
        def convertir_fecha_a_mes(fecha_str):
            try:
                if pd.isna(fecha_str):
                    return None
                if isinstance(fecha_str, str) and len(fecha_str) == 7 and '-' in fecha_str:
                    return fecha_str  # Ya está en formato YYYY-MM
                fecha = pd.to_datetime(fecha_str)
                return fecha.strftime('%Y-%m')
            except:
                return None

        processed_files = processing_summary.get("processed_files", {})
        turismo_files = processed_files.get("turismo", [])
        
        logger.info(f"📊 Total archivos de turismo procesados: {len(turismo_files)}")

        # Combinar todos los archivos ETI en un solo DataFrame por país
        all_tourism_data = []
        
        for file_info in turismo_files:
            try:
                logger.info(f"🔄 Cargando archivo de turismo: {file_info.get('original_file', 'unknown')}")
                df = pd.read_csv(file_info["processed_path"])
                
                logger.info(f"📊 Archivo cargado: {len(df)} filas, columnas: {list(df.columns)}")
                
                # Buscar columnas necesarias
                indice_col = None
                turistas_col = None
                pais_col = None
                
                for col in df.columns:
                    col_lower = col.lower()
                    if col_lower in ['indice_tiempo', 'fecha_std', 'periodo', 'año_mes']:
                        indice_col = col
                        break
                
                for col in df.columns:
                    col_lower = col.lower()
                    if any(keyword in col_lower for keyword in ['turistas', 'visitantes', 'no_residentes']):
                        turistas_col = col
                        break
                
                for col in df.columns:
                    col_lower = col.lower()
                    if any(keyword in col_lower for keyword in ['residencia', 'pais', 'country']):
                        pais_col = col
                        break
                
                if indice_col and turistas_col and pais_col:
                    logger.info(f"✅ Columnas encontradas: tiempo='{indice_col}', turistas='{turistas_col}', país='{pais_col}'")
                    
                    # Extraer datos necesarios
                    df_subset = df[[indice_col, turistas_col, pais_col]].copy()
                    df_subset.columns = ['indice_tiempo', 'turistas', 'pais_origen']
                    
                    # Limpiar y normalizar países
                    df_subset['pais_origen'] = df_subset['pais_origen'].astype(str).str.strip().str.title()
                    df_subset['turistas'] = pd.to_numeric(df_subset['turistas'], errors='coerce').fillna(0).astype(int)
                    
                    # Agregar fuente para tracking
                    df_subset['fuente'] = file_info.get('data_source', 'unknown')
                    
                    all_tourism_data.append(df_subset)
                    
                    logger.info(f"✅ Datos agregados: {len(df_subset)} registros de {file_info.get('original_file', 'unknown')}")
                    
                else:
                    logger.error(f"❌ Columnas faltantes en {file_info.get('original_file', 'unknown')}")
                    logger.error(f"  Tiempo: {indice_col}, Turistas: {turistas_col}, País: {pais_col}")
                    continue
                    
            except Exception as e:
                logger.error(f"❌ Error procesando archivo {file_info.get('original_file', 'unknown')}: {e}")
                continue

        if not all_tourism_data:
            logger.error("❌ No se pudieron procesar datos de turismo")
            return ""

        # Combinar todos los datos
        df_combined = pd.concat(all_tourism_data, ignore_index=True)
        logger.info(f"📊 Datos combinados: {len(df_combined)} registros totales")

        # AGREGACIÓN POR PAÍS Y TIEMPO - SUMAR TURISTAS DEL MISMO PAÍS
        logger.info("🔄 Agregando turistas por país de origen y mes...")
        
        df_aggregated = df_combined.groupby(['indice_tiempo', 'pais_origen'], as_index=False).agg({
            'turistas': 'sum'
        })
        
        logger.info(f"📊 Datos agregados por país: {len(df_aggregated)} registros únicos (tiempo x país)")
        
        # Mostrar estadísticas por país
        paises_stats = df_aggregated.groupby('pais_origen')['turistas'].agg(['sum', 'count']).sort_values('sum', ascending=False)
        logger.info(f"🌍 Top 10 países por total de turistas:")
        for pais, stats in paises_stats.head(10).iterrows():
            logger.info(f"  🌍 {pais}: {stats['sum']:,} turistas totales en {stats['count']} meses")

        # Convertir índice_tiempo a formato string consistente
        df_aggregated['indice_tiempo'] = df_aggregated['indice_tiempo'].apply(
            lambda x: convertir_fecha_a_mes(x) if pd.notna(x) else None
        )
        df_aggregated = df_aggregated[df_aggregated['indice_tiempo'].notna()]

        # Crear dataset con estructura país-mes
        logger.info(f"📊 Rango temporal: {df_aggregated['indice_tiempo'].min()} - {df_aggregated['indice_tiempo'].max()}")
        logger.info(f"🌍 Total países únicos: {df_aggregated['pais_origen'].nunique()}")
        
        # Ordenar por tiempo y país
        df_final = df_aggregated.copy()
        df_final = df_final.sort_values(['indice_tiempo', 'pais_origen'])

        # Merge con datos USD mensuales
        if usd_monthly.get("status") == "processed":
            logger.info("💰 Mergeando con datos USD mensuales...")
            
            usd_path = usd_monthly["monthly_path"]
            
            if Path(usd_path).exists():
                df_usd = pd.read_csv(usd_path)
                logger.info(f"📊 Datos USD: {len(df_usd)} meses")
                
                # Merge USD (se repite para cada país en el mismo mes)
                df_usd['indice_tiempo'] = df_usd['indice_tiempo'].astype(str)
                df_final['indice_tiempo'] = df_final['indice_tiempo'].astype(str)
                
                df_final = df_final.merge(df_usd, on="indice_tiempo", how="left")
                
                usd_matches = df_final['precio_promedio_usd'].notna().sum()
                logger.info(f"✅ Merge USD completado: {usd_matches}/{len(df_final)} registros con datos USD")
                
                # Variables USD
                if usd_matches > 0:
                    median_usd = df_final['precio_promedio_usd'].median()
                    df_final['usd_alto'] = (df_final['precio_promedio_usd'] > median_usd).astype(int)
                    
                    if 'variacion_usd_mensual' in df_final.columns:
                        median_variation = df_final['variacion_usd_mensual'].median()
                        df_final['usd_alta_variabilidad'] = (df_final['variacion_usd_mensual'] > median_variation).astype(int)
            else:
                logger.error("❌ Archivo USD no encontrado")
                # Agregar columnas vacías
                usd_columns = ['precio_promedio_usd', 'precio_minimo_usd', 'precio_maximo_usd', 
                              'variacion_usd_mensual', 'variacion_porcentual_usd', 'usd_alto', 'usd_alta_variabilidad']
                for col in usd_columns:
                    df_final[col] = None

        # Merge con datos de Google Trends mensuales  
        if trends_monthly.get("status") == "processed":
            logger.info("📈 Mergeando con datos de Google Trends mensuales...")
            
            trends_path = trends_monthly["monthly_path"]
            
            if Path(trends_path).exists():
                df_trends = pd.read_csv(trends_path)
                logger.info(f"📊 Datos Google Trends: {len(df_trends)} meses")
                
                # Merge Trends (se repite para cada país en el mismo mes)
                df_trends['indice_tiempo'] = df_trends['indice_tiempo'].astype(str)
                
                trends_cols = ['indice_tiempo', 'interes_google_promedio']
                if 'interes_alto' in df_trends.columns:
                    trends_cols.append('interes_alto')
                
                df_final = df_final.merge(df_trends[trends_cols], on="indice_tiempo", how="left")
                
                trends_matches = df_final['interes_google_promedio'].notna().sum()
                logger.info(f"✅ Merge Google Trends completado: {trends_matches}/{len(df_final)} registros con datos de interés")
                
                # Crear variable interes_alto si no existe
                if 'interes_alto' not in df_final.columns and 'interes_google_promedio' in df_final.columns:
                    median_interest = df_final['interes_google_promedio'].median()
                    df_final['interes_alto'] = (df_final['interes_google_promedio'] > median_interest).astype(int)
        else:
            logger.warning("⚠️ No hay datos de Google Trends procesados disponibles")
            df_final["interes_google_promedio"] = None
            df_final["interes_alto"] = None

        # Agregar variables temporales
        df_final[['año', 'mes']] = df_final['indice_tiempo'].apply(
            lambda x: pd.Series([int(x.split('-')[0]), int(x.split('-')[1])] if isinstance(x, str) and '-' in x else [None, None])
        )

        # Variables estacionales mensuales
        meses_nombres = ["enero","febrero","marzo","abril","mayo","junio","julio","agosto","septiembre","octubre","noviembre","diciembre"]
        for i in range(1, 13):
            df_final[f'es_{meses_nombres[i-1]}'] = (df_final['mes'] == i).astype(int)

        # Variables de eventos Mendoza
        df_final['mes_vendimia'] = df_final['mes'].isin([2, 3]).astype(int)
        df_final['vacaciones_invierno'] = df_final['mes'].isin([7, 8]).astype(int)
        df_final['temporada_alta'] = df_final['mes'].isin([1, 2, 3, 7, 8, 12]).astype(int)

        # Ordenar por tiempo y país (REQUISITO CUMPLIDO)
        df_final = df_final.sort_values(['indice_tiempo', 'pais_origen'])

        # Verificar estructura final
        logger.info(f"📊 Dataset final por país: {len(df_final)} registros")
        logger.info(f"📊 Estructura: {df_final['indice_tiempo'].nunique()} meses × {df_final['pais_origen'].nunique()} países")
        logger.info(f"📋 Columnas finales: {len(df_final.columns)}")

        # Guardar archivo final
        local_data_dir = Path("/usr/local/airflow/data/raw")
        local_data_dir.mkdir(parents=True, exist_ok=True)
        output_path = local_data_dir / "mendoza_turismo_por_pais_mensual.csv"
        
        df_final.to_csv(output_path, index=False, encoding="utf-8")
        
        logger.info("=" * 70)
        logger.info("📊 DATASET FINAL MENSUAL POR PAÍS CREADO CON ÉXITO")
        logger.info("=" * 70)
        logger.info(f"📁 Archivo: {output_path}")
        logger.info(f"📅 Meses únicos: {df_final['indice_tiempo'].nunique()}")
        logger.info(f"🌍 Países únicos: {df_final['pais_origen'].nunique()}")
        logger.info(f"📊 Total registros: {len(df_final)}")
        logger.info(f"🗓️ Rango: {df_final['indice_tiempo'].min()} - {df_final['indice_tiempo'].max()}")
        logger.info(f"📊 Total turistas: {df_final['turistas'].sum():,}")
        
        # Top países por turistas
        top_paises = df_final.groupby('pais_origen')['turistas'].sum().sort_values(ascending=False).head(5)
        logger.info("🌍 Top 5 países por turistas:")
        for pais, total in top_paises.items():
            logger.info(f"  📊 {pais}: {total:,} turistas")
        
        logger.info("✅ ORDENADO POR TIEMPO Y PAÍS - LISTO PARA ANÁLISIS")
        logger.info("=" * 70)

        return str(output_path)

    except Exception as e:
        logger.error(f"❌ Error creando dataset final por país: {e}")
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
    
    # 4. Procesar USD a promedios MENSUALES (renamed)
    usd_monthly = process_usd_to_monthly_averages(
        usd_data=usd_historical,
        directories=dirs
    )

    # 5. Procesar Google Trends MENSUAL (renamed)
    if trends_downloads:
        trends_monthly = process_google_trends_to_monthly(
            trends_data=trends_downloads[0],
            directories=dirs
        )
    else:
        # Crear tarea dummy que retorna status error
        @task
        def no_trends_available():
            return {"status": "error", "error": "No Google Trends configured"}
        
        trends_monthly = no_trends_available()

    # 6. Procesamiento tradicional
    processing_result = process_and_standardize_data(
        all_downloads=all_downloads,
        directories=dirs
    )

    # 7. Dataset final MENSUAL (renamed)
    final_dataset_monthly = create_final_monthly_dataset(
        processing_summary=processing_result,
        usd_monthly=usd_monthly,
        trends_monthly=trends_monthly,
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
    usd_historical >> usd_monthly
    
    # Google Trends processing (solo si hay trends_downloads)
    if trends_downloads:
        trends_downloads[0] >> trends_monthly
    
    # Processing depende de todas las descargas completadas
    for download_task in csv_downloads + api_downloads + trends_downloads:
        download_task >> processing_result
    
    # Dataset final depende de processing, USD y Trends
    processing_result >> final_dataset_monthly
    usd_monthly >> final_dataset_monthly
    trends_monthly >> final_dataset_monthly
    
    # Validation depende de todas las descargas y processing
    for download_task in csv_downloads + api_downloads + trends_downloads:
        download_task >> enhanced_validation
    processing_result >> enhanced_validation
    
    # Reporte final depende de validation y processing
    enhanced_validation >> final_enhanced_report
    processing_result >> final_enhanced_report