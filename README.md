README
Proyecto:Demanda Hotelera Argentina 

DAG: mza_turismo_etl_final
Objetivo: Automatizar la descarga, validación y reporte de datos de turismo en Argentina, integrando datos de YVERA y ETI para su posterior análisis exploratorio.

🔹 Descripción

Este pipeline realiza un ETL completo mensual con las siguientes funciones:

Preparación del entorno

Crea la estructura de carpetas por fecha (raw/, curated/, logs/, reports/) dentro de data/.

Descarga de datasets

Directa (CSV): descarga archivos CSV desde APIs o URLs conocidas.

Scraping dinámico: analiza páginas web con BeautifulSoup para encontrar enlaces a CSV y descargarlos automáticamente.

Validación de datos

Verifica tamaño de archivos, número de filas y columnas mínimas.

Evalúa calidad de datos: columnas vacías, cantidad mínima de filas (min_rows).

Genera reporte JSON con métricas de éxito y posibles problemas.

Reporte final del pipeline

Consolida resultados de ejecución y validación.

Proporciona resumen ejecutivo y próximos pasos (EDA, análisis exploratorio).