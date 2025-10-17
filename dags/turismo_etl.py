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
            logger.warning(f"⚠️ Archivo pequeño detectado: {name} - {total_size} bytes")
            try:
                df_test = pd.read_csv(dest_path)
                if len(df_test) > 0 and len(df_test.columns) > 2:
                    logger.info(f"✅ Archivo pequeño pero válido: {len(df_test)} filas")
                    return {
                        "src": src, "name": name, "path": str(dest_path),
                        "size": total_size, "status": "downloaded", "url": url,  # ✅ CAMBIO: siempre "downloaded"
                        "description": spec["description"], "category": category,
                        "frequency": frequency
                    }
                else:
                    logger.error(f"❌ Archivo muy pequeño: {len(df_test)} filas")
                    dest_path.unlink()
                    raise ValueError(f"Datos insuficientes: {len(df_test)} filas")
            except Exception as e:
                logger.error(f"❌ Error validando: {e}")
                dest_path.unlink()
                raise
        
        logger.info(f"✅ Descarga exitosa: {name} - {total_size:,} bytes")
        
        return {
            "src": src, "name": name, "path": str(dest_path),
            "size": total_size, "status": "downloaded", "url": url,
            "description": spec["description"], "category": category,
            "frequency": frequency  # ✅ CRÍTICO: incluir frecuencia
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
    """Procesa y estandariza todos los datos descargados - DEBUG EZEIZA."""
    try:
        # APLANAR LISTA DE DESCARGAS
        files = []
        for download in all_downloads:
            if isinstance(download, list):
                files.extend(download)
            else:
                files.append(download)
        
        logger.info(f"{'='*70}")
        logger.info(f"📥 TOTAL ARCHIVOS RECIBIDOS PARA PROCESAR: {len(files)}")
        logger.info(f"{'='*70}")
        
        # LOG DETALLADO de TODOS los archivos recibidos
        for idx, file_info in enumerate(files, 1):
            logger.info(f"{idx}. 📄 {file_info.get('name', 'unknown')}")
            logger.info(f"   - Status: {file_info.get('status', 'unknown')}")
            logger.info(f"   - Source: {file_info.get('src', 'unknown')}")
            logger.info(f"   - Category: {file_info.get('category', 'unknown')}")
            logger.info(f"   - Frequency: {file_info.get('frequency', 'unknown')}")
        
        processed_files = {
            "turismo": [],
            "economico": [], 
            "infraestructura": [],
            "general": [],
            "trends": []
        }
        
        processed_dir = Path(directories["processed"])
        
        # Mapeo de fuentes ETI a puntos de entrada
        PUNTO_ENTRADA_MAP = {
            "eti_aeropuerto": "Aeropuerto Mendoza",
            "eti_cristo_redentor": "Paso Cristo Redentor",
            "eti_ezeiza_aeroparque": "Aeropuerto Buenos Aires",
            "eti_cordoba_aeropuerto": "Aeropuerto Córdoba",
            "eti_puerto_buenos_aires": "Puerto Buenos Aires"
        }
        
        logger.info(f"🗺️ Puntos de entrada configurados: {list(PUNTO_ENTRADA_MAP.keys())}")
        
        def expand_quarterly_to_monthly_by_country(df, fecha_col, turistas_col, pais_col, punto_entrada, file_name):
            """Expande datos trimestrales a mensuales."""
            logger.info(f"🔄 Expandiendo TRIMESTRAL→MENSUAL para {punto_entrada}")
            logger.info(f"   📊 Registros originales: {len(df)}")
            
            expanded_rows = []
            
            for _, row in df.iterrows():
                fecha_trimestre = row[fecha_col]
                turistas_original = row[turistas_col]
                pais_origen = row[pais_col]
                
                turistas_por_mes = int(turistas_original / 3)
                
                año = fecha_trimestre.year
                mes_inicio = fecha_trimestre.month
                
                if mes_inicio in [1, 2, 3]:
                    meses = [1, 2, 3]
                elif mes_inicio in [4, 5, 6]:
                    meses = [4, 5, 6]
                elif mes_inicio in [7, 8, 9]:
                    meses = [7, 8, 9]
                else:
                    meses = [10, 11, 12]
                
                for mes in meses:
                    fecha_mensual = pd.Timestamp(year=año, month=mes, day=1)
                    indice_mensual = fecha_mensual.strftime('%Y-%m')
                    
                    expanded_rows.append({
                        fecha_col: fecha_mensual,
                        turistas_col: turistas_por_mes,
                        pais_col: pais_origen,
                        'punto_entrada': punto_entrada,
                        'indice_tiempo': indice_mensual,
                        'fecha_std': fecha_mensual
                    })
            
            df_expanded = pd.DataFrame(expanded_rows)
            df_expanded[turistas_col] = df_expanded[turistas_col].astype(int)
            
            logger.info(f"✅ Expandido: {len(df)} → {len(df_expanded)} registros mensuales")
            return df_expanded
        
        # CONTADOR
        archivos_turismo_procesados = 0
        archivos_turismo_saltados = 0
        
        for file_info in files:
            status = file_info.get("status", "")
            
            # ✅ ACEPTAR TANTO "downloaded" COMO "cached"
            if not (status.startswith("downloaded") or status == "cached"):
                logger.warning(f"⚠️ Saltando archivo con status '{status}': {file_info.get('name', 'unknown')}")
                archivos_turismo_saltados += 1
                continue
            
            path = file_info.get("path", "")
            if not path or not Path(path).exists():
                logger.error(f"❌ Ruta inválida: {path}")
                archivos_turismo_saltados += 1
                continue
            
            category = file_info.get("category", "general")
            src = file_info.get("src", "")
            
            # 🔍 DETECTAR SI ES EZEIZA PARA DEBUG ULTRA-DETALLADO
            es_ezeiza = src == "eti_ezeiza_aeroparque"
            
            logger.info(f"{'='*60}")
            logger.info(f"🔄 PROCESANDO: {file_info.get('name', 'unknown')}")
            logger.info(f"   📂 Categoría: {category}")
            logger.info(f"   🏷️ Source: {src}")
            if es_ezeiza:
                logger.info(f"   🚨 DEBUG MODE: EZEIZA/AEROPARQUE DETECTADO")
            
            try:
                if path.endswith(".csv"):
                    df = pd.read_csv(path, encoding='utf-8')
                    logger.info(f"   📊 CSV cargado: {len(df)} filas × {len(df.columns)} columnas")
                    
                    # 🔍 DEBUG EZEIZA: Mostrar TODAS las columnas
                    if es_ezeiza:
                        logger.info(f"   🚨 EZEIZA - Columnas completas: {list(df.columns)}")
                        logger.info(f"   🚨 EZEIZA - Primeras 5 filas:")
                        for idx, row in df.head(5).iterrows():
                            logger.info(f"      {idx}: {dict(row)}")
                    
                    # PROCESAR ARCHIVOS ETI
                    if category == "turismo":
                        logger.info(f"   🎯 Archivo de turismo ETI detectado")
                        
                        # IDENTIFICAR PUNTO DE ENTRADA
                        punto_entrada = PUNTO_ENTRADA_MAP.get(src, None)
                        
                        if not punto_entrada:
                            logger.error(f"   ❌ NO se pudo identificar punto_entrada para src='{src}'")
                            archivos_turismo_saltados += 1
                            continue
                        
                        logger.info(f"   🚪 PUNTO DE ENTRADA: {punto_entrada}")
                        
                        # FRECUENCIA
                        frequency = file_info.get("frequency", "unknown")
                        es_trimestral = frequency == "trimestral"
                        
                        logger.info(f"   📅 Frecuencia: {frequency} ({'TRIMESTRAL' if es_trimestral else 'MENSUAL'})")
                        
                        # BUSCAR COLUMNAS
                        fecha_col = None
                        turistas_col = None
                        pais_col = None
                        
                        logger.info(f"   📋 Columnas disponibles: {list(df.columns)}")
                        
                        # Buscar fecha
                        for col in df.columns:
                            if any(kw in col.lower() for kw in ['indice_tiempo', 'anio_trimestre', 'año_trimestre', 
                                                                  'trimestre', 'periodo', 'fecha']):
                                fecha_col = col
                                logger.info(f"   ✅ Fecha: '{col}'")
                                break
                        
                        # Buscar turistas (PRIORIZAR turistas_no_residentes)
                        for col in df.columns:
                            if col.lower() == 'turistas_no_residentes':
                                turistas_col = col
                                logger.info(f"   ✅ Turistas (EXACTO): '{col}'")
                                break
                        
                        if not turistas_col:
                            for col in df.columns:
                                if any(kw in col.lower() for kw in ['turistas', 'visitantes', 'no_residentes']):
                                    turistas_col = col
                                    logger.info(f"   ✅ Turistas: '{col}'")
                                    break
                        
                        # 🔍 DEBUG EZEIZA: Verificar qué columna se eligió
                        if es_ezeiza:
                            logger.info(f"   🚨 EZEIZA - Columna turistas seleccionada: '{turistas_col}'")
                            if turistas_col:
                                logger.info(f"   🚨 EZEIZA - Tipo de dato: {df[turistas_col].dtype}")
                                logger.info(f"   🚨 EZEIZA - Valores únicos (muestra): {df[turistas_col].unique()[:10]}")
                                logger.info(f"   🚨 EZEIZA - Suma TOTAL original: {df[turistas_col].sum():,}")
                        
                        # Buscar país
                        for col in df.columns:
                            if any(kw in col.lower() for kw in ['pais_de_residencia', 'residencia', 'pais', 'country']):
                                pais_col = col
                                logger.info(f"   ✅ País: '{col}'")
                                break
                        
                        if not all([fecha_col, turistas_col, pais_col]):
                            logger.error(f"   ❌ COLUMNAS FALTANTES:")
                            logger.error(f"      Fecha: {fecha_col}, Turistas: {turistas_col}, País: {pais_col}")
                            archivos_turismo_saltados += 1
                            continue
                        
                        # MOSTRAR MUESTRA
                        logger.info(f"   📊 MUESTRA ORIGINAL (primeras 3 filas):")
                        for idx, row in df.head(3).iterrows():
                            logger.info(f"      {pais_col}={row[pais_col]}, {turistas_col}={row[turistas_col]}, {fecha_col}={row[fecha_col]}")
                        
                        # 🔍 DEBUG EZEIZA: Estadísticas ANTES de limpiar
                        if es_ezeiza:
                            logger.info(f"   🚨 EZEIZA - ANTES de limpiar:")
                            logger.info(f"      Total filas: {len(df)}")
                            logger.info(f"      Total turistas: {df[turistas_col].sum():,}")
                            logger.info(f"      Promedio: {df[turistas_col].mean():.0f}")
                            logger.info(f"      Max: {df[turistas_col].max():,}")
                            logger.info(f"      Min: {df[turistas_col].min():,}")
                        
                        # LIMPIAR
                        df[pais_col] = df[pais_col].astype(str).str.strip().str.title()
                        df[turistas_col] = pd.to_numeric(df[turistas_col], errors='coerce').fillna(0).astype(int)
                        
                        logger.info(f"   📊 TOTAL turistas ANTES: {df[turistas_col].sum():,}")
                        
                        # 🔍 DEBUG EZEIZA: Después de conversión numérica
                        if es_ezeiza:
                            logger.info(f"   🚨 EZEIZA - DESPUÉS de conversión numérica:")
                            logger.info(f"      Total turistas: {df[turistas_col].sum():,}")
                            logger.info(f"      Valores nulos: {df[turistas_col].isna().sum()}")
                            logger.info(f"      Valores cero: {(df[turistas_col] == 0).sum()}")
                        
                        # PROCESAR FECHAS
                        df[fecha_col] = pd.to_datetime(df[fecha_col], errors='coerce')
                        df = df[df[fecha_col].notna()]
                        
                        # 🔍 DEBUG EZEIZA: Después de filtro de fechas nulas
                        if es_ezeiza:
                            logger.info(f"   🚨 EZEIZA - DESPUÉS de filtro fechas nulas:")
                            logger.info(f"      Filas restantes: {len(df)}")
                            logger.info(f"      Total turistas: {df[turistas_col].sum():,}")
                        
                        df = df[df[fecha_col] >= '2014-01-01']
                        
                        logger.info(f"   📅 Registros después filtro 2014: {len(df)}")
                        
                        # 🔍 DEBUG EZEIZA: Después de filtro 2014
                        if es_ezeiza:
                            logger.info(f"   🚨 EZEIZA - DESPUÉS de filtro 2014:")
                            logger.info(f"      Filas restantes: {len(df)}")
                            logger.info(f"      Total turistas: {df[turistas_col].sum():,}")
                            logger.info(f"      Fechas min/max: {df[fecha_col].min()} / {df[fecha_col].max()}")
                        
                        # EXPANDIR O MANTENER
                        if es_trimestral:
                            logger.info(f"   🔄 EXPANDIENDO trimestral→mensual")
                            df = expand_quarterly_to_monthly_by_country(
                                df, fecha_col, turistas_col, pais_col, punto_entrada, file_info['name']
                            )
                        else:
                            logger.info(f"   📊 MENSUAL - conservando valores")
                            df['indice_tiempo'] = df[fecha_col].dt.strftime('%Y-%m')
                            df['fecha_std'] = df[fecha_col]
                            df['punto_entrada'] = punto_entrada
                            df = df.rename(columns={turistas_col: 'turistas', pais_col: 'pais_origen'})
                        
                        # 🔍 DEBUG EZEIZA: Después de expansión/renombrar
                        if es_ezeiza:
                            logger.info(f"   🚨 EZEIZA - DESPUÉS de procesamiento temporal:")
                            logger.info(f"      Filas finales: {len(df)}")
                            if 'turistas' in df.columns:
                                logger.info(f"      Total turistas: {df['turistas'].sum():,}")
                            else:
                                logger.info(f"      Total turistas (col original): {df[turistas_col].sum():,}")
                            logger.info(f"      Columnas finales: {list(df.columns)}")
                            
                            # MUESTRA DETALLADA 2014-02
                            sample_2014_02 = df[df['indice_tiempo'] == '2014-02']
                            if len(sample_2014_02) > 0:
                                logger.info(f"   🚨 EZEIZA - MUESTRA 2014-02 (5 filas):")
                                for idx, row in sample_2014_02.head(5).iterrows():
                                    pais = row.get('pais_origen', 'N/A')
                                    turistas_val = row.get('turistas', 0)
                                    logger.info(f"      {pais}: {turistas_val:,} turistas")
                                logger.info(f"   🚨 EZEIZA - Total 2014-02: {sample_2014_02['turistas'].sum():,} turistas")
                        
                        # ASEGURAR INT
                        if 'turistas' not in df.columns:
                            df['turistas'] = df[turistas_col].astype(int)
                        else:
                            df['turistas'] = df['turistas'].astype(int)
                        
                        logger.info(f"   ✅ PROCESADO EXITOSO para {punto_entrada}")
                        logger.info(f"   📊 TOTAL turistas DESPUÉS: {df['turistas'].sum():,}")
                        logger.info(f"   📊 Registros finales: {len(df)}")
                        
                        archivos_turismo_procesados += 1
                
                elif path.endswith(".json"):
                    # Procesar JSON (USD, etc)
                    with open(path, 'r', encoding='utf-8') as f:
                        json_data = json.load(f)
                    
                    if file_info.get("src") == "dolarapi":
                        logger.info(f"💰 Procesando USD")
                        df = pd.DataFrame(json_data)
                        
                        fecha_col = next((col for col in df.columns if col.lower() in ['fecha', 'date']), None)
                        if fecha_col:
                            df['fecha_std'] = pd.to_datetime(df[fecha_col], errors='coerce')
                            logger.info(f"✅ USD: {len(df)} registros")
                
                # VERIFICAR NO VACÍO
                if df.empty:
                    logger.warning(f"   ⚠️ DataFrame vacío")
                    continue
                
                # FILTRAR 2014+
                if 'fecha_std' in df.columns:
                    df = df[df['fecha_std'] >= '2014-01-01']
                
                if df.empty:
                    logger.warning(f"   ⚠️ Vacío después de filtros")
                    continue
                
                # GUARDAR
                output_path = processed_dir / category / f"processed_{file_info['name'].replace('.json', '.csv')}"
                df.to_csv(output_path, index=False, encoding='utf-8')
                
                logger.info(f"   💾 Guardado: {output_path.name}")
                
                # 🔍 DEBUG EZEIZA: Verificar archivo guardado
                if es_ezeiza:
                    logger.info(f"   🚨 EZEIZA - Archivo guardado: {output_path}")
                    logger.info(f"   🚨 EZEIZA - Tamaño archivo: {output_path.stat().st_size:,} bytes")
                    # Re-leer para verificar
                    df_verificacion = pd.read_csv(output_path)
                    logger.info(f"   🚨 EZEIZA - Verificación post-guardado:")
                    logger.info(f"      Filas guardadas: {len(df_verificacion)}")
                    if 'turistas' in df_verificacion.columns:
                        logger.info(f"      Total turistas guardados: {df_verificacion['turistas'].sum():,}")
                
                processed_files[category].append({
                    "original_file": file_info["name"],
                    "processed_path": str(output_path),
                    "rows": len(df),
                    "columns": len(df.columns),
                    "has_punto_entrada": 'punto_entrada' in df.columns,
                    "punto_entrada_value": df['punto_entrada'].iloc[0] if 'punto_entrada' in df.columns and len(df) > 0 else None,
                    "total_turistas": int(df['turistas'].sum()) if 'turistas' in df.columns else 0,
                    "data_source": file_info.get("src", "unknown"),
                    "frequency": file_info.get("frequency", "unknown")
                })
                
            except Exception as e:
                logger.error(f"   ❌ Error: {e}")
                import traceback
                logger.error(traceback.format_exc())
                continue
        
        # RESUMEN
        logger.info(f"{'='*70}")
        logger.info(f"📊 RESUMEN DE PROCESAMIENTO")
        logger.info(f"{'='*70}")
        logger.info(f"✅ Archivos ETI procesados: {archivos_turismo_procesados}")
        logger.info(f"⚠️ Archivos ETI saltados: {archivos_turismo_saltados}")
        
        for cat, files_list in processed_files.items():
            if cat == "turismo" and files_list:
                logger.info(f"\n🎯 TURISMO ({len(files_list)} archivos):")
                total_turistas = 0
                for f in files_list:
                    turistas = f.get('total_turistas', 0)
                    total_turistas += turistas
                    logger.info(f"  • {f['punto_entrada_value']}: {turistas:,} turistas")
                logger.info(f"  📊 TOTAL TURISTAS: {total_turistas:,}")
        
        summary = {
            "timestamp": datetime.now().isoformat(),
            "files_by_category": {cat: len(files) for cat, files in processed_files.items()},
            "total_processed": sum(len(files) for files in processed_files.values()),
            "processed_files": processed_files,
            "success": True,
            "archivos_turismo_procesados": archivos_turismo_procesados,
            "archivos_turismo_saltados": archivos_turismo_saltados
        }
        
        summary_path = processed_dir / "processing_summary.json"
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        
        return summary
        
    except Exception as e:
        logger.error(f"❌ Error en procesamiento: {e}")
        import traceback
        logger.error(traceback.format_exc())
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
    """Crea dataset final MENSUAL con PUNTO DE ENTRADA - DEBUGGING MEJORADO."""
    try:
        logger.info("🎯 Creando dataset final MENSUAL con TIEMPO × PAÍS × PUNTO_ENTRADA...")

        def convertir_fecha_a_mes(fecha_str):
            try:
                if pd.isna(fecha_str):
                    return None
                if isinstance(fecha_str, str) and len(fecha_str) == 7 and '-' in fecha_str:
                    return fecha_str
                fecha = pd.to_datetime(fecha_str)
                return fecha.strftime('%Y-%m')
            except:
                return None

        processed_files = processing_summary.get("processed_files", {})
        turismo_files = processed_files.get("turismo", [])
        
        logger.info(f"📊 Total archivos de turismo procesados: {len(turismo_files)}")

        all_tourism_data = []
        
        for file_info in turismo_files:
            try:
                file_name = file_info.get('original_file', 'unknown')
                file_path = file_info["processed_path"]
                
                logger.info(f"🔄 Cargando: {file_name}")
                logger.info(f"   📁 Ruta: {file_path}")
                
                df = pd.read_csv(file_path)
                
                logger.info(f"   📊 Archivo cargado: {len(df)} filas × {len(df.columns)} columnas")
                logger.info(f"   📋 Columnas: {list(df.columns)}")
                
                # 🔍 DEBUG: Mostrar estadísticas ANTES de filtrar columnas
                if 'turistas' in df.columns:
                    total_turistas_antes = df['turistas'].sum()
                    logger.info(f"   💰 Total turistas ANTES de filtrar columnas: {total_turistas_antes:,}")
                
                # Buscar columnas necesarias
                indice_col = None
                turistas_col = None
                pais_col = None
                punto_col = None
                
                for col in df.columns:
                    col_lower = col.lower()
                    if col_lower in ['indice_tiempo', 'fecha_std', 'periodo', 'año_mes']:
                        indice_col = col
                    elif col_lower in ['turistas', 'visitantes', 'no_residentes', 'turistas_no_residentes']:
                        turistas_col = col
                    elif col_lower in ['pais_origen', 'residencia', 'pais', 'country', 'pais_de_residencia']:
                        pais_col = col
                    elif col_lower == 'punto_entrada':
                        punto_col = col
                
                logger.info(f"   🔍 Columnas identificadas:")
                logger.info(f"      - Tiempo: {indice_col}")
                logger.info(f"      - Turistas: {turistas_col}")
                logger.info(f"      - País: {pais_col}")
                logger.info(f"      - Punto entrada: {punto_col}")
                
                if indice_col and turistas_col and pais_col and punto_col:
                    logger.info(f"   ✅ Columnas completas encontradas")
                    
                    # MANTENER LAS 4 COLUMNAS CLAVE
                    df_subset = df[[indice_col, turistas_col, pais_col, punto_col]].copy()
                    df_subset.columns = ['indice_tiempo', 'turistas', 'pais_origen', 'punto_entrada']
                    
                    # 🔍 DEBUG: Verificar ANTES de limpiar
                    logger.info(f"   📊 ANTES de limpiar:")
                    logger.info(f"      Total turistas: {df_subset['turistas'].sum():,}")
                    logger.info(f"      Países únicos: {df_subset['pais_origen'].nunique()}")
                    logger.info(f"      Puntos únicos: {df_subset['punto_entrada'].nunique()}")
                    
                    # Limpiar datos
                    df_subset['pais_origen'] = df_subset['pais_origen'].astype(str).str.strip().str.title()
                    df_subset['punto_entrada'] = df_subset['punto_entrada'].astype(str).str.strip()
                    df_subset['turistas'] = pd.to_numeric(df_subset['turistas'], errors='coerce').fillna(0).astype(int)
                    
                    # 🔍 DEBUG: Verificar DESPUÉS de limpiar
                    logger.info(f"   📊 DESPUÉS de limpiar:")
                    logger.info(f"      Total turistas: {df_subset['turistas'].sum():,}")
                    logger.info(f"      Valores nulos en turistas: {df_subset['turistas'].isna().sum()}")
                    logger.info(f"      Valores cero: {(df_subset['turistas'] == 0).sum()}")
                    
                    # 🔍 MOSTRAR MUESTRA POR PUNTO DE ENTRADA
                    punto_entrada_value = df_subset['punto_entrada'].iloc[0]
                    logger.info(f"   🚪 Punto entrada: {punto_entrada_value}")
                    logger.info(f"   📊 Total turistas para este punto: {df_subset['turistas'].sum():,}")
                    
                    all_tourism_data.append(df_subset)
                    
                    logger.info(f"   ✅ Agregados {len(df_subset)} registros con {df_subset['turistas'].sum():,} turistas")
                    
                else:
                    logger.error(f"   ❌ Columnas faltantes en {file_name}")
                    logger.error(f"      Disponibles: {list(df.columns)}")
                    logger.error(f"      Buscadas: tiempo={indice_col}, turistas={turistas_col}, país={pais_col}, punto={punto_col}")
                    continue
                    
            except Exception as e:
                logger.error(f"❌ Error procesando {file_name}: {e}")
                import traceback
                logger.error(traceback.format_exc())
                continue

        if not all_tourism_data:
            logger.error("❌ No se pudieron procesar datos de turismo")
            return ""

        logger.info(f"📊 Total DataFrames a combinar: {len(all_tourism_data)}")
        
        # 🔍 DEBUG: Mostrar totales ANTES de combinar
        for idx, df_temp in enumerate(all_tourism_data):
            punto = df_temp['punto_entrada'].iloc[0] if len(df_temp) > 0 else 'N/A'
            total = df_temp['turistas'].sum()
            logger.info(f"   {idx+1}. {punto}: {total:,} turistas en {len(df_temp)} registros")

        df_combined = pd.concat(all_tourism_data, ignore_index=True)
        logger.info(f"📊 Datos combinados: {len(df_combined)} registros totales")
        logger.info(f"💰 Total turistas COMBINADOS: {df_combined['turistas'].sum():,}")

        # VERIFICAR ESTRUCTURA ANTES DE AGREGAR
        logger.info(f"📋 Columnas del DataFrame combinado: {list(df_combined.columns)}")
        logger.info(f"📊 Tipos de datos:")
        for col in df_combined.columns:
            logger.info(f"   - {col}: {df_combined[col].dtype}")
        
        # MUESTRA DE DATOS ANTES DE AGREGAR
        logger.info(f"📋 Muestra de datos ANTES de agregar (primeras 5 filas):")
        for idx, row in df_combined.head(5).iterrows():
            logger.info(f"   {row['indice_tiempo']} | {row['pais_origen']} | {row['punto_entrada']} | {row['turistas']:,} turistas")

        # AGREGACIÓN POR TIEMPO + PAÍS + PUNTO_ENTRADA
        logger.info("🔄 Agregando por tiempo × país × punto_entrada...")
        
        # 🔍 DEBUG: Verificar valores únicos antes de agrupar
        logger.info(f"📊 Valores únicos ANTES de agrupar:")
        logger.info(f"   - Índices tiempo: {df_combined['indice_tiempo'].nunique()}")
        logger.info(f"   - Países: {df_combined['pais_origen'].nunique()}")
        logger.info(f"   - Puntos entrada: {df_combined['punto_entrada'].nunique()}")
        
        df_aggregated = df_combined.groupby(
            ['indice_tiempo', 'pais_origen', 'punto_entrada'], 
            as_index=False
        ).agg({
            'turistas': 'sum'
        })
        
        logger.info(f"📊 Registros únicos (tiempo × país × punto): {len(df_aggregated)}")
        logger.info(f"💰 Total turistas DESPUÉS de agregar: {df_aggregated['turistas'].sum():,}")
        
        # 🔍 DEBUG: Verificar si se perdieron datos en la agregación
        diferencia = df_combined['turistas'].sum() - df_aggregated['turistas'].sum()
        if abs(diferencia) > 1:
            logger.error(f"❌ PÉRDIDA DE DATOS EN AGREGACIÓN: {diferencia:,} turistas")
        else:
            logger.info(f"✅ No se perdieron datos en la agregación")
        
        # Estadísticas por punto de entrada
        puntos_stats = df_aggregated.groupby('punto_entrada')['turistas'].agg(['sum', 'count']).sort_values('sum', ascending=False)
        logger.info(f"🚪 Estadísticas por punto de entrada DESPUÉS de agregar:")
        for punto, stats in puntos_stats.iterrows():
            logger.info(f"  🚪 {punto}: {stats['sum']:,} turistas en {stats['count']} registros")

        # Convertir índice_tiempo
        df_aggregated['indice_tiempo'] = df_aggregated['indice_tiempo'].apply(
            lambda x: convertir_fecha_a_mes(x) if pd.notna(x) else None
        )
        df_aggregated = df_aggregated[df_aggregated['indice_tiempo'].notna()]

        logger.info(f"📊 Rango temporal: {df_aggregated['indice_tiempo'].min()} - {df_aggregated['indice_tiempo'].max()}")
        logger.info(f"🌍 Países únicos: {df_aggregated['pais_origen'].nunique()}")
        logger.info(f"🚪 Puntos de entrada únicos: {df_aggregated['punto_entrada'].nunique()}")
        
        # Ordenar por TIEMPO, PAÍS Y PUNTO DE ENTRADA
        df_final = df_aggregated.copy()
        df_final = df_final.sort_values(['indice_tiempo', 'pais_origen', 'punto_entrada'])

        # Merge con USD (se repite para cada combinación)
        if usd_monthly.get("status") == "processed":
            logger.info("💰 Mergeando con datos USD...")
            
            usd_path = usd_monthly["monthly_path"]
            
            if Path(usd_path).exists():
                df_usd = pd.read_csv(usd_path)
                df_usd['indice_tiempo'] = df_usd['indice_tiempo'].astype(str)
                df_final['indice_tiempo'] = df_final['indice_tiempo'].astype(str)
                
                df_final = df_final.merge(df_usd, on="indice_tiempo", how="left")
                
                usd_matches = df_final['precio_promedio_usd'].notna().sum()
                logger.info(f"✅ Merge USD: {usd_matches}/{len(df_final)} registros")
                
                if usd_matches > 0:
                    median_usd = df_final['precio_promedio_usd'].median()
                    df_final['usd_alto'] = (df_final['precio_promedio_usd'] > median_usd).astype(int)
                    
                    if 'variacion_usd_mensual' in df_final.columns:
                        median_variation = df_final['variacion_usd_mensual'].median()
                        df_final['usd_alta_variabilidad'] = (df_final['variacion_usd_mensual'] > median_variation).astype(int)
            else:
                logger.error("❌ Archivo USD no encontrado")
                usd_columns = ['precio_promedio_usd', 'precio_minimo_usd', 'precio_maximo_usd', 
                              'variacion_usd_mensual', 'variacion_porcentual_usd', 'usd_alto', 'usd_alta_variabilidad']
                for col in usd_columns:
                    df_final[col] = None

        # Merge con Google Trends
        if trends_monthly.get("status") == "processed":
            logger.info("📈 Mergeando con Google Trends...")
            
            trends_path = trends_monthly["monthly_path"]
            
            if Path(trends_path).exists():
                df_trends = pd.read_csv(trends_path)
                df_trends['indice_tiempo'] = df_trends['indice_tiempo'].astype(str)
                
                trends_cols = ['indice_tiempo', 'interes_google_promedio']
                if 'interes_alto' in df_trends.columns:
                    trends_cols.append('interes_alto')
                
                df_final = df_final.merge(df_trends[trends_cols], on="indice_tiempo", how="left")
                
                trends_matches = df_final['interes_google_promedio'].notna().sum()
                logger.info(f"✅ Merge Trends: {trends_matches}/{len(df_final)} registros")
                
                if 'interes_alto' not in df_final.columns and 'interes_google_promedio' in df_final.columns:
                    median_interest = df_final['interes_google_promedio'].median()
                    df_final['interes_alto'] = (df_final['interes_google_promedio'] > median_interest).astype(int)
        else:
            logger.warning("⚠️ No hay datos de Google Trends")
            df_final["interes_google_promedio"] = None
            df_final["interes_alto"] = None

        # Variables temporales
        df_final[['año', 'mes']] = df_final['indice_tiempo'].apply(
            lambda x: pd.Series([int(x.split('-')[0]), int(x.split('-')[1])] if isinstance(x, str) and '-' in x else [None, None])
        )

        # Variables estacionales
        meses_nombres = ["enero","febrero","marzo","abril","mayo","junio","julio","agosto","septiembre","octubre","noviembre","diciembre"]
        for i in range(1, 13):
            df_final[f'es_{meses_nombres[i-1]}'] = (df_final['mes'] == i).astype(int)

        # Eventos Mendoza
        df_final['mes_vendimia'] = df_final['mes'].isin([2, 3]).astype(int)
        df_final['vacaciones_invierno'] = df_final['mes'].isin([7, 8]).astype(int)
        df_final['temporada_alta'] = df_final['mes'].isin([1, 2, 3, 7, 8, 12]).astype(int)

        # ORDENAMIENTO FINAL
        df_final = df_final.sort_values(['indice_tiempo', 'pais_origen', 'punto_entrada'])

        # Guardar archivo final
        local_data_dir = Path("/usr/local/airflow/data/raw")
        local_data_dir.mkdir(parents=True, exist_ok=True)
        output_path = local_data_dir / "mendoza_turismo_detallado_mensual.csv"
        
        df_final.to_csv(output_path, index=False, encoding="utf-8")
        
        logger.info("=" * 70)
        logger.info("📊 DATASET FINAL DETALLADO CREADO CON ÉXITO")
        logger.info("=" * 70)
        logger.info(f"📁 Archivo: {output_path}")
        logger.info(f"📅 Meses únicos: {df_final['indice_tiempo'].nunique()}")
        logger.info(f"🌍 Países únicos: {df_final['pais_origen'].nunique()}")
        logger.info(f"🚪 Puntos de entrada únicos: {df_final['punto_entrada'].nunique()}")
        logger.info(f"📊 Total registros: {len(df_final)} (tiempo × país × punto)")
        logger.info(f"🗓️ Rango: {df_final['indice_tiempo'].min()} - {df_final['indice_tiempo'].max()}")
        logger.info(f"📊 Total turistas: {df_final['turistas'].sum():,}")
        
        # Top combinaciones
        top_combos = df_final.groupby(['pais_origen', 'punto_entrada'])['turistas'].sum().sort_values(ascending=False).head(5)
        logger.info("🔝 Top 5 combinaciones (país × punto):")
        for (pais, punto), total in top_combos.items():
            logger.info(f"  📊 {pais} → {punto}: {total:,} turistas")
        
        logger.info("✅ ORDENADO: TIEMPO → PAÍS → PUNTO_ENTRADA")
        logger.info("=" * 70)

        return str(output_path)

    except Exception as e:
        logger.error(f"❌ Error creando dataset detallado: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
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