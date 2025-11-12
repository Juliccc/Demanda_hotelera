"""
════════════════════════════════════════════════════════════════════════════════
APLICACIÓN STREAMLIT - PREDICCIÓN DE TURISMO EN MENDOZA
════════════════════════════════════════════════════════════════════════════════
Aplicación interactiva para explorar datos, visualizar resultados y realizar
predicciones de demanda turística en Mendoza, Argentina.

Autor: Juliccc
Fecha: 2025-11-04
Entrega: 4 - 
════════════════════════════════════════════════════════════════════════════════
"""

import streamlit as st
import pandas as pd
import numpy as np
import joblib
import json
import altair as alt
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go

# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURACIÓN DE LA PÁGINA
# ═══════════════════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="Predictor Turismo Argentina",
    page_icon="🏔️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ═══════════════════════════════════════════════════════════════════════════
# ESTILOS PERSONALIZADOS
# ═══════════════════════════════════════════════════════════════════════════
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        padding: 1rem 0;
        background: linear-gradient(90deg, #e3f2fd 0%, #ffffff 100%);
        border-radius: 10px;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 10px;
        border-left: 5px solid #1f77b4;
    }
    .info-box {
        background-color: rgb(255 46 126 / 33%);
        padding: 1rem;
        border-radius: 5px;
        border-left: 4px solid #2196f3;
        margin: 1rem 0;
    }
    .warning-box {
        background-color: #c19c28;
        padding: 1rem;
        border-radius: 5px;
        border-left: 4px solid #ff9800;
        margin: 1rem 0;
    }
    .success-box {
        background-color: #26bf4b;
        padding: 1rem;
        border-radius: 5px;
        border-left: 4px solid #28a745;
        margin: 1rem 0;
    }
    .stButton>button {
        width: 100%;
        background-color: #1f77b4;
        color: white;
        font-weight: bold;
        border-radius: 5px;
        padding: 0.5rem 1rem;
        border: none;
        transition: all 0.3s;
    }
    .stButton>button:hover {
        background-color: #1565c0;
        box-shadow: 0 4px 8px rgba(0,0,0,0.2);
    }
</style>
""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════════════
# FUNCIONES DE CARGA DE DATOS Y MODELO
# ═══════════════════════════════════════════════════════════════════════════

@st.cache_resource
def cargar_modelo():
    """Carga el modelo entrenado desde disco"""
    try:
        modelo = joblib.load('models/modelo_turismo_mendoza_final.pkl')
        return modelo
    except FileNotFoundError:
        st.error("❌ Error: No se encontró el archivo del modelo.")
        return None

@st.cache_data
def cargar_metadata():
    """Carga los metadatos del modelo"""
    try:
        with open('models/modelo_metadata.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return None

@st.cache_data
def cargar_stats():
    """Carga las estadísticas del target"""
    try:
        with open('models/target_stats.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return None

@st.cache_data
def cargar_datos_muestra():
    """Carga datos de muestra para exploración"""
    try:
        return pd.read_csv('models/sample_data.csv')
    except FileNotFoundError:
        return None

@st.cache_data
def cargar_datos_completos():
    """Carga el dataset completo CON FILTRADO por metadata"""
    try:
        # Cargar dataset completo
        df = pd.read_csv('mendoza_turismo_final_filtrado.csv', sep=';', encoding='utf-8')
        df = df[df['turistas'] > 0].copy()
        
        # NUEVO: Filtrar por categorías válidas del metadata
        metadata_temp = cargar_metadata()
        if metadata_temp and 'categorias_unicas' in metadata_temp.get('features', {}):
            paises_validos = metadata_temp['features']['categorias_unicas']['pais_origen']
            puntos_validos = metadata_temp['features']['categorias_unicas']['punto_entrada']
            
            # Aplicar filtros
            df_limpio = df[
                df['pais_origen'].isin(paises_validos) &
                df['punto_entrada'].isin(puntos_validos)
            ].copy()
            
            registros_eliminados = len(df) - len(df_limpio)
            if registros_eliminados > 0:
                st.info(f"🧹 Dataset limpiado: {registros_eliminados:,} registros duplicados eliminados")
            
            st.success(f"✅ Cargados {len(df_limpio):,} registros | {df_limpio['turistas'].sum():,.0f} turistas totales")
            return df_limpio
        else:
            st.warning("⚠️ No se encontró metadata. Usando dataset sin filtrar.")
            return df
            
    except FileNotFoundError:
        st.error("❌ Archivo 'mendoza_turismo_final_filtrado.csv' no encontrado")
        return cargar_datos_muestra()

# ═══════════════════════════════════════════════════════════════════════════
# CARGAR RECURSOS AL INICIO
# ═══════════════════════════════════════════════════════════════════════════
modelo = cargar_modelo()
metadata = cargar_metadata()
stats = cargar_stats()
df_sample = cargar_datos_muestra()
df_full = cargar_datos_completos()

# ═══════════════════════════════════════════════════════════════════════════
# ENCABEZADO PRINCIPAL
# ═══════════════════════════════════════════════════════════════════════════
st.markdown('<div class="main-header">🏔️ Predictor de Turismo en Argentina</div>', unsafe_allow_html=True)

st.markdown("""
<div class="info-box">
    📊 <strong>Aplicación de Machine Learning</strong> para predecir la demanda turística mensual en Argentina.
    Basada en datos históricos de 2014-2025 y modelos de regresión entrenados con Scikit-learn.
</div>
""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════════════
# SIDEBAR - NAVEGACIÓN
# ═══════════════════════════════════════════════════════════════════════════
st.sidebar.image("https://upload.wikimedia.org/wikipedia/commons/thumb/1/14/Bandera_de_Mendoza.svg/320px-Bandera_de_Mendoza.svg.png", width=200)
st.sidebar.title("Navegación")
st.sidebar.markdown("---")
fecha_actual = datetime.now()

pagina = st.sidebar.radio(
    "Selecciona una sección:",
    [
        "🏠 Inicio",
        "📊 Exploración de Datos",
        "📈 Visualizaciones",
        "🤖 Información del Modelo",
        "🔮 Hacer Predicciones"
    ],
    index=0
)


st.sidebar.markdown("---")

meses_es = {
    1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril',
    5: 'Mayo', 6: 'Junio', 7: 'Julio', 8: 'Agosto',
    9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'
}

st.sidebar.markdown(f"""
### 👤 Autor
**Julian Cadenas**

### 📅 Fecha
{fecha_actual.strftime("%d de %B, %Y")}

### 🎓 Proyecto
Entrega 4 - Visualización e Integración  
*Ciencia de Datos*
""")

# ═══════════════════════════════════════════════════════════════════════════
# PÁGINA 1: INICIO
# ═══════════════════════════════════════════════════════════════════════════
if pagina == "🏠 Inicio":
    st.header("🏠 Bienvenido al Predictor de Turismo en Mendoza")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📋 Sobre este Proyecto")
        st.markdown("""
        Esta aplicación es el resultado de un proyecto completo de **Machine Learning** 
        para predecir la demanda turística mensual en Argentina.
        
        **Objetivo:**
        - Predecir el número de turistas que visitarán las provincias centrales en un mes específico
        - Ayudar en la planificación hotelera y gestión de recursos turísticos
        - Proporcionar insights sobre tendencias y patrones de turismo
        
        **Características:**
        - ✅ Pipeline completo de Scikit-learn
        - ✅ Modelo optimizado con validación cruzada
        - ✅ Visualizaciones interactivas con Altair
        - ✅ Interfaz amigable para predicciones
        """)
    
    with col2:
        st.subheader("📊 Estadísticas del Dataset")
        
        if df_full is not None:
            st.metric("Total de Registros", f"{len(df_full):,}")
            
            col2a, col2b = st.columns(2)
            with col2a:
                st.metric("Período", "2014-2025")
                st.metric("Media de Turistas", f"{df_full['turistas'].mean():,.0f}")
            with col2b:
                if 'año' in df_full.columns:
                    st.metric("Años Analizados", df_full['año'].nunique())
                if 'pais_origen' in df_full.columns:
                    st.metric("Puntos de Origen", df_full['pais_origen'].nunique())
    
    st.markdown("---")
    
    # Información del modelo
    if metadata:
        st.subheader("🤖 Modelo Entrenado")
        
        col3, col4, col5, col6 = st.columns(4)
        
        with col3:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric(
                "Modelo", 
                metadata['model_info']['nombre'],
                help="Algoritmo de Machine Learning utilizado"
            )
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col4:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric(
                "Test RMSE", 
                f"{metadata['metricas']['test_rmse']:,.0f}",
                help="Root Mean Squared Error en conjunto de prueba"
            )
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col5:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric(
                "Test R²", 
                f"{metadata['metricas']['test_r2']:.3f}",
                help="Coeficiente de determinación (varianza explicada)"
            )
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col6:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric(
                "Test MAE", 
                f"{metadata['metricas']['test_mae']:,.0f}",
                help="Mean Absolute Error en conjunto de prueba"
            )
            st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown(f"""
        <div class="success-box">
            ✅ <strong>Modelo Validado:</strong> El modelo explica el {metadata['metricas']['test_r2']*100:.1f}% 
            de la variabilidad en el número de turistas. Error promedio de {metadata['metricas']['test_mae']:,.0f} turistas.
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    st.info("👈 Usa el menú lateral para navegar entre las diferentes secciones de la aplicación.")

# ═══════════════════════════════════════════════════════════════════════════
# PÁGINA 2: EXPLORACIÓN DE DATOS
# ═══════════════════════════════════════════════════════════════════════════
elif pagina == "📊 Exploración de Datos":
    st.header("📊 Exploración de Datos")
    
    if df_full is not None:
        st.subheader("🔍 Vista General del Dataset")
        
        # Tabs para diferentes vistas
        tab1, tab2, tab3, tab4 = st.tabs(["📋 Datos", "📈 Estadísticas", "🗺️ Distribuciones", "🔗 Correlaciones"])
        
        with tab1:
            st.markdown("### Primeros registros del dataset")
            st.dataframe(df_full.head(100), use_container_width=True)
            
            st.markdown("### Información del Dataset")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Filas", f"{len(df_full):,}")
            with col2:
                st.metric("Columnas", df_full.shape[1])

        
        with tab2:
            st.markdown("### Estadísticas Descriptivas")
            st.dataframe(df_full.describe(), use_container_width=True)
            
            st.markdown("### Estadísticas de la Variable Objetivo (Turistas)")
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Media", f"{df_full['turistas'].mean():,.0f}")
            with col2:
                st.metric("Mediana", f"{df_full['turistas'].median():,.0f}")
            with col3:
                st.metric("Mínimo", f"{df_full['turistas'].min():,.0f}")
            with col4:
                st.metric("Máximo", f"{df_full['turistas'].max():,.0f}")
        
        with tab3:
            st.markdown("### Distribución de Turistas")
            
            # Histograma interactivo con Plotly
            fig = px.histogram(
                df_full, 
                x='turistas', 
                nbins=50,
                title="Distribución del Número de Turistas",
                labels={'turistas': 'Número de Turistas', 'count': 'Frecuencia'},
                color_discrete_sequence=['#1f77b4']
            )
            fig.update_layout(showlegend=False, height=400)
            st.plotly_chart(fig, use_container_width=True)
            
            # Boxplot
            if 'año' in df_full.columns:
                st.markdown("### Distribución por Año")
                fig2 = px.box(
                    df_full, 
                    x='año', 
                    y='turistas',
                    title="Distribución de Turistas por Año",
                    labels={'turistas': 'Número de Turistas', 'año': 'Año'},
                    color='año',
                    color_discrete_sequence=px.colors.qualitative.Set3
                )
                fig2.update_layout(showlegend=False, height=400)
                st.plotly_chart(fig2, use_container_width=True)
        
        with tab4:
            st.markdown("### Matriz de Correlación")
            
            # Seleccionar solo columnas numéricas
            numeric_cols = df_full.select_dtypes(include=[np.number]).columns.tolist()
            
            if len(numeric_cols) > 1:
                # Calcular correlación
                corr_matrix = df_full[numeric_cols].corr()
                
                # Heatmap con Plotly
                fig = px.imshow(
                    corr_matrix,
                    labels=dict(color="Correlación"),
                    x=corr_matrix.columns,
                    y=corr_matrix.columns,
                    color_continuous_scale='RdBu_r',
                    zmin=-1, zmax=1,
                    title="Matriz de Correlación entre Variables Numéricas"
                )
                fig.update_layout(height=600)
                st.plotly_chart(fig, use_container_width=True)
                
                # Top correlaciones con turistas
                if 'turistas' in corr_matrix.columns:
                    st.markdown("### Top 10 Variables más Correlacionadas con Turistas")
                    correlaciones_turistas = corr_matrix['turistas'].drop('turistas').sort_values(ascending=False).head(10)
                    
                    fig2 = px.bar(
                        x=correlaciones_turistas.values,
                        y=correlaciones_turistas.index,
                        orientation='h',
                        title="Correlaciones con Variable Objetivo",
                        labels={'x': 'Correlación', 'y': 'Variable'},
                        color=correlaciones_turistas.values,
                        color_continuous_scale='RdBu_r'
                    )
                    fig2.update_layout(showlegend=False, height=400)
                    st.plotly_chart(fig2, use_container_width=True)
            else:
                st.warning("No hay suficientes variables numéricas para calcular correlaciones.")
    
    else:
        st.error("❌ No se pudieron cargar los datos. Verifica que el archivo exista.")

# ═══════════════════════════════════════════════════════════════════════════
# PÁGINA 3: VISUALIZACIONES INTERACTIVAS CON ALTAIR
# ═══════════════════════════════════════════════════════════════════════════
elif pagina == "📈 Visualizaciones":
    st.header("📈 Visualizaciones Interactivas")
    
    
    if df_full is not None and 'año' in df_full.columns and 'mes' in df_full.columns:
        
        # ═══════════════════════════════════════════════════════════════════
        # VISUALIZACIÓN 1: EVOLUCIÓN TEMPORAL
        # ═══════════════════════════════════════════════════════════════════
        st.subheader("📈 1. Evolución Temporal del Turismo")
        
        # Preparar datos temporales
        df_temporal = df_full.groupby(['año', 'mes']).agg({'turistas': 'sum'}).reset_index()
        df_temporal['fecha'] = pd.to_datetime(
            df_temporal['año'].astype(str) + '-' + 
            df_temporal['mes'].astype(str).str.zfill(2) + '-01'
        )
        df_temporal['periodo'] = df_temporal['año'].apply(
            lambda x: 'Pandemia COVID-19' if x in [2020, 2021] 
            else 'Pre-Pandemia (2014-2019)' if x < 2020 
            else 'Post-Pandemia (2022+)'
        )
        
        # Gráfico Altair
        line_chart = alt.Chart(df_temporal).mark_line(
            point=True,
            strokeWidth=2.5
        ).encode(
            x=alt.X('fecha:T', title='Fecha', axis=alt.Axis(format='%Y')),
            y=alt.Y('turistas:Q', title='Número de Turistas', axis=alt.Axis(format=',')),
            color=alt.Color('periodo:N',
                           title='Período',
                           scale=alt.Scale(
                               domain=['Pre-Pandemia (2014-2019)', 'Pandemia COVID-19', 'Post-Pandemia (2022+)'],
                               range=['#2c7bb6', '#d7191c', '#1a9850']
                           )),
            tooltip=[
                alt.Tooltip('fecha:T', title='Fecha', format='%B %Y'),
                alt.Tooltip('turistas:Q', title='Turistas', format=','),
                alt.Tooltip('periodo:N', title='Período')
            ]
        ).properties(
            width=800,
            height=400,
            title='Evolución Temporal del Turismo en Mendoza (2014-2025)'
        ).interactive()
        
        st.altair_chart(line_chart, use_container_width=True)
        
        st.markdown("""
        **💡 Insights:**
        - 🔴 Fuerte caída en 2020-2021 debido a la pandemia COVID-19
        - 🟢 Recuperación progresiva desde 2022
        - 📊 Tendencia general al crecimiento pre-pandemia
        """)
        
        st.markdown("---")
        
        # ═══════════════════════════════════════════════════════════════════
        # VISUALIZACIÓN 2+3 FUSIONADA: PAÍSES CON CLICK INTERACTIVO
        # ═══════════════════════════════════════════════════════════════════
        st.subheader("🌍 2. Análisis de Países de Origen y Estacionalidad")

        if 'pais_origen' in df_full.columns:
            
            # INICIALIZAR SESSION STATE (CRÍTICO)
            if 'pais_seleccionado_viz' not in st.session_state:
                st.session_state.pais_seleccionado_viz = None
            
            # Filtrar por metadata ANTES de agrupar
            if metadata and 'categorias_unicas' in metadata.get('features', {}):
                paises_validos = metadata['features']['categorias_unicas']['pais_origen']
            else:
                paises_validos = [
                    "Bolivia", "Brasil", "Chile", "EEUU, Canadá y México",
                    "Europa y Resto del Mundo", "Paraguay", "Resto de América", "Uruguay"
                ]
            
            # Filtrar dataset
            df_paises_limpio = df_full[df_full['pais_origen'].isin(paises_validos)]
            
            # Agrupar (ahora máximo 8 países)
            top_paises = df_paises_limpio.groupby('pais_origen').agg({'turistas': 'sum'}).reset_index()
            top_paises = top_paises.sort_values('turistas', ascending=False)
            top_paises_list = top_paises['pais_origen'].tolist()
            
            st.caption(f"✅ Mostrando {len(top_paises)} países según metadata del modelo")

            # ───────────────────────────────────────────────────────────────
            # INICIALIZAR SESSION STATE (¡CRÍTICO - ANTES DE USARLO!)
            # ───────────────────────────────────────────────────────────────
        

            # ───────────────────────────────────────────────────────────────
            # GRÁFICO DE PAÍSES (CON CLICK)
            # ───────────────────────────────────────────────────────────────

            st.markdown(f"### 📊 Top {len(top_paises)} Países de Origen")
            st.caption("👆 **Click en una barra** para ver su estacionalidad mensual")
            
            # Selector manual y reset
            col_selector, col_reset = st.columns([4, 1])
            
            with col_selector:
                pais_manual = st.selectbox(
                    "O selecciona manualmente:",
                    options=[None] + top_paises_list,
                    format_func=lambda x: "-- Ninguno --" if x is None else x,
                    key='selector_manual_pais',
                    index=0 if st.session_state.pais_seleccionado_viz is None else 
                        (top_paises_list.index(st.session_state.pais_seleccionado_viz) + 1 
                        if st.session_state.pais_seleccionado_viz in top_paises_list else 0)
                )
                
                if pais_manual:
                    st.session_state.pais_seleccionado_viz = pais_manual
            
            with col_reset:
                if st.button("🔄 Reset", help="Limpiar selección"):
                    st.session_state.pais_seleccionado_viz = None
                    st.rerun()
            
            # ───────────────────────────────────────────────────────────────
            # CREAR SELECTION DE ALTAIR (IMPORTANTE!)
            # ───────────────────────────────────────────────────────────────
            
            # Definir selection para capturar clicks
            click_selection = alt.selection_point(
                fields=['pais_origen'],
                empty=False,
                name='pais_click'
            )
            
            # Highlight visual del país seleccionado
            top_paises['seleccionado'] = top_paises['pais_origen'] == st.session_state.pais_seleccionado_viz
            
            # Crear gráfico con selection
            paises_chart = alt.Chart(top_paises).mark_bar(
                cursor='pointer'
            ).encode(
                x=alt.X('turistas:Q', 
                    title='Total de Turistas', 
                    axis=alt.Axis(format=',')),
                y=alt.Y('pais_origen:N', 
                    title='País de Origen', 
                    sort='-x'),
                color=alt.condition(
                    alt.datum.seleccionado == True,
                    alt.value('#FF6B6B'),  # Rojo si está seleccionado
                    alt.Color('turistas:Q', 
                            scale=alt.Scale(scheme='viridis'), 
                            legend=None)
                ),
                opacity=alt.condition(
                    alt.datum.seleccionado == True,
                    alt.value(1.0),
                    alt.value(0.8)
                ),
                tooltip=[
                    alt.Tooltip('pais_origen:N', title='País'),
                    alt.Tooltip('turistas:Q', title='Total Turistas', format=',')
                ]
            ).add_params(
                click_selection  # ← AGREGAR SELECTION AQUÍ
            ).properties(
                height=400,
                title='Total de Turistas por País (Click para ver estacionalidad)'
            )
            
            # Renderizar con captura de clicks
            event = st.altair_chart(
                paises_chart, 
                use_container_width=True, 
                on_select="rerun",
                key='chart_paises'
            )
            
            # ───────────────────────────────────────────────────────────────
            # CAPTURAR CLICK Y ACTUALIZAR ESTADO
            # ───────────────────────────────────────────────────────────────
            
            if event and event.selection and 'pais_click' in event.selection:
                selection_data = event.selection['pais_click']
                if selection_data:
                    # Extraer país clickeado
                    if isinstance(selection_data, list) and len(selection_data) > 0:
                        pais_clickeado = selection_data[0].get('pais_origen')
                        if pais_clickeado:
                            st.session_state.pais_seleccionado_viz = pais_clickeado
                            st.rerun()
            
            st.markdown("""
            **💡 Sobre países:**
            - 🌎 Principales mercados turísticos identificados
            - 🎯 Base para estrategias de marketing focalizadas
            - 📊 Click en una barra para análisis detallado por mes
            """)
            
            st.markdown("---")
            
            # ───────────────────────────────────────────────────────────────
            # MOSTRAR ESTACIONALIDAD SI HAY PAÍS SELECCIONADO
            # ───────────────────────────────────────────────────────────────
            
            if st.session_state.pais_seleccionado_viz:
                pais = st.session_state.pais_seleccionado_viz
                
                st.markdown(f"### 📈 Estacionalidad de **{pais}**")
                
                # Filtrar datos por país
                df_pais = df_full[df_full['pais_origen'] == pais]
                
                if len(df_pais) > 0:
                    df_mensual = df_pais.groupby('mes').agg({'turistas': 'mean'}).reset_index()
                    
                    # Agregar nombres de meses
                    df_mensual['mes_nombre'] = df_mensual['mes'].map({
                        1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril',
                        5: 'Mayo', 6: 'Junio', 7: 'Julio', 8: 'Agosto',
                        9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'
                    })
                    
                    # Gráfico de barras mensual
                    bar_chart = alt.Chart(df_mensual).mark_bar(
                        cornerRadiusTopLeft=5,
                        cornerRadiusTopRight=5
                    ).encode(
                        x=alt.X('mes_nombre:N', 
                            title='Mes', 
                            sort=[
                                'Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio',
                                'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre'
                            ],
                            axis=alt.Axis(labelAngle=-45)),
                        y=alt.Y('turistas:Q', 
                            title='Promedio de Turistas', 
                            axis=alt.Axis(format=',')),
                        color=alt.Color('turistas:Q', 
                                    scale=alt.Scale(scheme='blues'), 
                                    legend=None),
                        tooltip=[
                            alt.Tooltip('mes_nombre:N', title='Mes'),
                            alt.Tooltip('turistas:Q', title='Promedio Turistas', format=',d')
                        ]
                    ).properties(
                        height=400,
                        title=f'Promedio Mensual de Turistas de {pais}'
                    )
                    
                    st.altair_chart(bar_chart, use_container_width=True)
                    
                    # ───────────────────────────────────────────────────────
                    # MÉTRICAS DEL PAÍS SELECCIONADO
                    # ───────────────────────────────────────────────────────
                    
                    if len(df_mensual) > 0:
                        mes_max = df_mensual.loc[df_mensual['turistas'].idxmax()]
                        mes_min = df_mensual.loc[df_mensual['turistas'].idxmin()]
                        promedio = df_mensual['turistas'].mean()
                        
                        col1, col2, col3, col4 = st.columns(4)
                        
                        with col1:
                            st.metric(
                                "🔥 Mes Pico",
                                mes_max['mes_nombre'],
                                f"{mes_max['turistas']:,.0f}"
                            )
                        
                        with col2:
                            st.metric(
                                "📉 Mes Más Bajo",
                                mes_min['mes_nombre'],
                                f"{mes_min['turistas']:,.0f}"
                            )
                        
                        with col3:
                            variacion = ((mes_max['turistas'] - mes_min['turistas']) / mes_min['turistas']) * 100
                            st.metric(
                                "📊 Variación",
                                f"{variacion:.0f}%",
                                "pico vs bajo"
                            )
                        
                        with col4:
                            st.metric(
                                "📅 Promedio",
                                f"{promedio:,.0f}",
                                "turistas/mes"
                            )
                    
                    # Insights específicos del país
                    st.info(f"""
                    **💡 Análisis de {pais}:**
                    - **Mejor mes:** {mes_max['mes_nombre']} con {mes_max['turistas']:,.0f} turistas
                    - **Peor mes:** {mes_min['mes_nombre']} con {mes_min['turistas']:,.0f} turistas
                    - **Variación estacional:** {variacion:.0f}% de diferencia
                    - **Promedio mensual:** {promedio:,.0f} turistas
                    """)
                else:
                    st.warning(f"⚠️ No hay datos suficientes para {pais}")
                
            else:
                # Mensaje si no hay país seleccionado
                st.info("👆 **Selecciona un país** en el gráfico de arriba o en el dropdown para ver su estacionalidad mensual")
            
            st.markdown("---")
            
            # ───────────────────────────────────────────────────────────────
            # INSIGHTS GENERALES (SIEMPRE VISIBLES)
            # ───────────────────────────────────────────────────────────────
            
            with st.expander("💡 Ver Insights Generales"):
                st.markdown("""
                ### Análisis Combinado de Países y Estacionalidad
                
                **Sobre Países de Origen:**
                - 🥇 **Brasil** lidera como principal mercado emisor
                - 🇪🇺 **Europa** muestra alto potencial de crecimiento
                - 🇨🇱 **Chile** mantiene flujo constante (proximidad)
                - 🇺🇾 **Uruguay** presenta estacionalidad marcada
                
                **Patrones Estacionales Generales:**
                - 🌞 **Verano (Dic-Feb):** Temporada alta natural
                - 🍷 **Marzo:** Pico por Fiesta de la Vendimia
                - ❄️ **Invierno (May-Ago):** Baja demanda excepto julio (nieve)
                - 🌸 **Primavera (Sep-Nov):** Recuperación gradual
                
                **Recomendaciones Estratégicas:**
                - 🎯 **Marketing dirigido:** Campañas específicas por país y temporada
                - 💰 **Pricing dinámico:** Ajustar tarifas según demanda esperada
                - 👥 **Gestión de RRHH:** Planificar contrataciones estacionales
                - 📦 **Gestión de stock:** Anticipar necesidades por país/mes
                """)

        else:
            st.warning("⚠️ Datos de 'pais_origen' no disponibles.")

# ═══════════════════════════════════════════════════════════════════════════
# PÁGINA 4: INFORMACIÓN DEL MODELO
# ═══════════════════════════════════════════════════════════════════════════
elif pagina == "🤖 Información del Modelo":
    st.header("🤖 Información del Modelo")
    
    if metadata:
        # Información general
        st.subheader("📋 Detalles del Modelo")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### Configuración")
            st.json({
                "Nombre": metadata['model_info']['nombre'],
                "Tipo": metadata['model_info']['tipo'],
                "Framework": metadata['model_info']['framework'],
                "Versión Sklearn": metadata['model_info']['version_sklearn']
            })
            
            st.markdown("### Dataset")
            st.json({
                "Registros Train": metadata['dataset_info']['n_registros_train'],
                "Registros Test": metadata['dataset_info']['n_registros_test'],
                "Features": metadata.get('dataset_info', {}).get('n_features', 'N/A'),
                "Fecha Corte Train/Test": metadata['dataset_info']['fecha_corte_train_test'],
                
            })
        
        with col2:
            st.markdown("### Métricas de Performance")
            
            metricas = metadata['metricas']
            
            st.metric(
                "Test RMSE",
                f"{metricas['test_rmse']:,.0f}",
                help="Error cuadrático medio en conjunto de prueba"
            )
            
            st.metric(
                "Test MAE",
                f"{metricas['test_mae']:,.0f}",
                help="Error absoluto medio en conjunto de prueba"
            )
            
            st.metric(
                "Test R²",
                f"{metricas['test_r2']:.4f}",
                delta=f"{metricas['test_r2']*100:.1f}% varianza explicada",
                help="Coeficiente de determinación"
            )
            
            st.metric(
                "Overfitting Ratio",
                f"{metricas['overfitting_ratio']:.2f}x",
                help="Ratio Test RMSE / Train RMSE"
            )
        
        st.markdown("---")
        
        # Preprocesamiento
        st.subheader("🔧 Pipeline de Preprocesamiento")
        
        col3, col4 = st.columns(2)
        
        with col3:
            st.markdown("### Variables Numéricas")
            st.write(f"**Total:** {len(metadata['features']['numericas'])} variables")
            with st.expander("Ver lista completa"):
                for feat in metadata['features']['numericas']:
                    st.write(f"- {feat}")
            
            st.markdown("**Preprocesamiento:**")
            st.code(f"""
1. Imputación: {metadata['preprocessing']['numeric_imputer']}
2. Escalado: {metadata['preprocessing']['numeric_scaler']}
            """)
        
        with col4:
            st.markdown("### Variables Categóricas")
            st.write(f"**Total:** {len(metadata['features']['categoricas'])} variables")
            with st.expander("Ver lista completa"):
                for feat in metadata['features']['categoricas']:
                    st.write(f"- {feat}")
            
            st.markdown("**Preprocesamiento:**")
            st.code(f"""
1. Imputación: {metadata['preprocessing']['categorical_imputer']}
2. Encoding: {metadata['preprocessing']['categorical_encoder']}
            """)
        
        st.markdown("---")
        
        # Información de entrenamiento
        st.subheader("⏱️ Información de Entrenamiento")
        st.json(metadata['training_info'])
        
        # Interpretación de métricas
        st.markdown("---")
        st.subheader("📊 Interpretación de las Métricas")
        
        r2 = metricas['test_r2']
        rmse = metricas['test_rmse']
        
        st.markdown(f"""
        **R² = {r2:.4f}:**
        - El modelo explica el **{r2*100:.1f}%** de la variabilidad en el número de turistas
        - El {(1-r2)*100:.1f}% restante se debe a factores no incluidos en el modelo o ruido inherente
        
        **RMSE = {rmse:,.0f} turistas:**
        - En promedio, las predicciones se desvían por **±{rmse:,.0f} turistas** del valor real
        - Para un promedio de ~11,500 turistas, esto representa un error del {(rmse/11500)*100:.1f}%
        
        **Calidad del Modelo:**
        """)
        
        if r2 >= 0.7:
            st.success(f"✅ **Excelente**: R² ≥ 0.70 indica un modelo con muy buena capacidad predictiva")
        elif r2 >= 0.5:
            st.info(f"✓ **Bueno**: R² ≥ 0.50 indica un modelo con capacidad predictiva aceptable")
        else:
            st.warning(f"⚠️ **Regular**: R² < 0.50 sugiere limitaciones en la capacidad predictiva")
    
    else:
        st.error("❌ No se pudieron cargar los metadatos del modelo.")

# ═══════════════════════════════════════════════════════════════════════════
# PÁGINA 5: HACER PREDICCIONES
# ═══════════════════════════════════════════════════════════════════════════
elif pagina == "🔮 Hacer Predicciones":
    st.header("🔮 Realizar Predicciones")
    
    st.markdown("""
    <div class="info-box">
        📝 Ingresa solo las variables principales. Las variables derivadas se calcularán automáticamente.
        💡 <strong>Nuevo:</strong> Puedes predecir para un país específico o para todos los países en conjunto.
    </div>
    """, unsafe_allow_html=True)
    
    if modelo and metadata:
        # Obtener features del metadata
        numeric_features = metadata['features']['numericas']
        categorical_features = metadata['features']['categoricas']
        
        # ═══════════════════════════════════════════════════════════════════
        # FORMULARIO SIMPLIFICADO
        # ═══════════════════════════════════════════════════════════════════
        
        with st.form("form_prediccion"):
            col1, col2, col3 = st.columns(3)
            
            # ══════════════════════════════════════════════════════════════
            # COLUMNA 1: VARIABLES TEMPORALES
            # ══════════════════════════════════════════════════════════════
            with col1:
                st.markdown("### 📅 Fecha")
                
                año_input = st.number_input(
                    "Año",
                    min_value=2024,
                    max_value=2030,
                    value=2025,
                    step=1,
                    help="Año para el cual deseas predecir"
                )
                
                mes_input = st.selectbox(
                    "Mes",
                    options=list(range(1, 13)),
                    index=datetime.now().month - 1,
                    format_func=lambda x: [
                        'Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio',
                        'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre'
                    ][x-1],
                    help="Mes para el cual deseas predecir"
                )
                
                # Información del mes
                mes_nombre = [
                    'Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio',
                    'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre'
                ][mes_input - 1]
                
                st.markdown("---")
                st.markdown("#### 📊 Características:")
                
                # Características del mes
                es_temporada_alta = mes_input in [12, 1, 2]
                es_vendimia = mes_input == 3
                es_vacaciones_invierno = mes_input == 7
                
                if es_temporada_alta:
                    st.success("🔥 Temporada Alta")
                elif es_vendimia:
                    st.info("🍇 Vendimia")
                elif es_vacaciones_invierno:
                    st.info("❄️ Vacaciones Invierno")
                else:
                    st.warning("📉 Temporada Baja")
            
            # ══════════════════════════════════════════════════════════════
            # COLUMNA 2: ORIGEN Y ENTRADA (CORREGIDO - USA METADATA)
            # ══════════════════════════════════════════════════════════════
            with col2:
                st.markdown("### 🌍 Origen")
        
                # ═══════════════════════════════════════════════════════════
                # PAÍS DE ORIGEN - USAR METADATA (CATEGORÍAS LIMPIAS)
                # ═══════════════════════════════════════════════════════════
                
                paises_disponibles = []
                
                # PRIORIDAD 1: Usar metadata (SIEMPRE PRIMERO)
                if metadata and 'categorias_unicas' in metadata.get('features', {}):
                    paises_disponibles = metadata['features']['categorias_unicas'].get('pais_origen', [])
                    st.caption(f"✅ {len(paises_disponibles)} países del modelo")
                
                # PRIORIDAD 2: df_full solo como fallback
                elif df_full is not None and 'pais_origen' in df_full.columns:
                    paises_disponibles = sorted(df_full['pais_origen'].unique().tolist())
                    st.caption(f" Usando dataset ({len(paises_disponibles)} países - puede tener duplicados)")
                
                # PRIORIDAD 3: Valores por defecto limpios
                else:
                    paises_disponibles = [
                        'Brasil',
                        'Chile',
                        'EEUU, Canadá y México',
                        'Europa y Resto del Mundo',
                        'Paraguay',
                        'Resto de América',
                        'Uruguay'
                    ]
                    st.caption(f"ℹ️ Valores por defecto")
                
                # AGREGAR OPCIÓN "TODOS"
                opciones_pais = ['🌎 Todos los países (Total)'] + paises_disponibles
                
                pais_seleccion = st.selectbox(
                    "País de Origen",
                    options=opciones_pais,
                    help="Selecciona un país específico o 'Todos' para predecir el total"
                )
                
                # Detectar si seleccionó "Todos"
                predecir_todos_paises = pais_seleccion.startswith('🌎')
                
                if predecir_todos_paises:
                    st.info(f"📊 Se predecirá para {len(paises_disponibles)} países")
                
                st.markdown("### 🚪 Entrada")
                
                # ═══════════════════════════════════════════════════════════
                # PUNTO DE ENTRADA - USAR METADATA (CATEGORÍAS LIMPIAS)
                # ═══════════════════════════════════════════════════════════
                
                puntos_disponibles = []
                
                # PRIORIDAD 1: Usar metadata (SIEMPRE PRIMERO)
                if metadata and 'categorias_unicas' in metadata.get('features', {}):
                    puntos_disponibles = metadata['features']['categorias_unicas'].get('punto_entrada', [])
                    st.caption(f"✅ {len(puntos_disponibles)} puntos del modelo")
                
                # PRIORIDAD 2: df_full solo como fallback
                elif df_full is not None and 'punto_entrada' in df_full.columns:
                    puntos_disponibles = sorted(df_full['punto_entrada'].unique().tolist())
                    st.caption(f"⚠️ Usando dataset")
                
                # PRIORIDAD 3: Valores por defecto
                else:
                    puntos_disponibles = [
                        'Aeropuerto Buenos Aires',
                        'Aeropuerto Córdoba',
                        'Aeropuerto Mendoza',
                        'Paso Cristo Redentor',
                        'Puerto Buenos Aires'
                    ]
                    st.caption(f"ℹ️ Valores por defecto")
                
                # AGREGAR OPCIÓN "TODOS" PARA PUNTOS
                opciones_punto = ['🚪 Todos los puntos de entrada'] + puntos_disponibles
                
                punto_seleccion = st.selectbox(
                    "Punto de Entrada",
                    options=opciones_punto,
                    help="Selecciona un punto específico o 'Todos'"
                )
                
                predecir_todos_puntos = punto_seleccion.startswith('🚪')
                
                if predecir_todos_puntos:
                    st.info(f"📊 Se predecirá para {len(puntos_disponibles)} puntos")
            
            # ══════════════════════════════════════════════════════════════
            # COLUMNA 3: ECONÓMICAS E INFO
            # ══════════════════════════════════════════════════════════════
            with col3:
                st.markdown("### 💰 Económico")
                
                precio_input = st.number_input(
                    "Precio del dolar",
                    min_value=0.0,
                    max_value=5000.0,
                    value=1000.0,
                    step=50.0,
                    help="Precio promedio del alojamiento"
                )
                
                st.markdown("---")
                st.markdown("### 📊 Histórico")
                
                if stats:
                    st.metric("Media", f"{stats['train']['mean']:,.0f}")
                    st.metric("Máximo", f"{stats['train']['max']:,.0f}")
            
            # ══════════════════════════════════════════════════════════════
            # BOTÓN DE PREDICCIÓN
            # ══════════════════════════════════════════════════════════════
            st.markdown("---")
            submitted = st.form_submit_button("🔮 Predecir", use_container_width=True)
            
            if submitted:
                try:
                    # ═══════════════════════════════════════════════════════
                    # FUNCIÓN AUXILIAR: CREAR INPUT DATA
                    # ═══════════════════════════════════════════════════════
                    
                    def crear_input_data(pais, punto):
                        """Crea diccionario de input para un país y punto específico"""
                        input_data = {}
                        
                        # Variables manuales
                        if 'año' in numeric_features:
                            input_data['año'] = año_input
                        if 'mes' in numeric_features:
                            input_data['mes'] = mes_input
                        if 'precio_promedio_usd' in numeric_features:
                            input_data['precio_promedio_usd'] = precio_input
                        if 'pais_origen' in categorical_features:
                            input_data['pais_origen'] = pais
                        if 'punto_entrada' in categorical_features:
                            input_data['punto_entrada'] = punto
                        
                        # Dummies de meses
                        meses_nombres = ['enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio',
                                        'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre']
                        
                        for i, mes_nom in enumerate(meses_nombres, 1):
                            posibles = [f'es_{mes_nom}', f'Es{mes_nom.capitalize()}']
                            for nombre_var in posibles:
                                if nombre_var in numeric_features:
                                    input_data[nombre_var] = 1 if mes_input == i else 0
                                elif nombre_var in categorical_features:
                                    input_data[nombre_var] = 'Si' if mes_input == i else 'No'
                        
                        # Variables de temporada
                        posibles_temp_alta = ['temporada_alta', 'TemporadaAlta']
                        for nombre_var in posibles_temp_alta:
                            if nombre_var in numeric_features:
                                input_data[nombre_var] = 1 if mes_input in [12, 1, 2] else 0
                        
                        posibles_vendimia = ['vendimia', 'Vendimia']
                        for nombre_var in posibles_vendimia:
                            if nombre_var in numeric_features:
                                input_data[nombre_var] = 1 if mes_input == 3 else 0
                        
                        posibles_vac = ['vacaciones_invierno', 'VacacionesInvierno']
                        for nombre_var in posibles_vac:
                            if nombre_var in numeric_features:
                                input_data[nombre_var] = 1 if mes_input == 7 else 0
                        
                        # Resto de variables numéricas
                        for feat in numeric_features:
                            if feat not in input_data:
                                if df_full is not None and feat in df_full.columns:
                                    input_data[feat] = float(df_full[feat].median())
                                else:
                                    if 'precio' in feat.lower() or 'usd' in feat.lower():
                                        input_data[feat] = 850.0
                                    else:
                                        input_data[feat] = 0.0
                        
                        # Resto de variables categóricas
                        for feat in categorical_features:
                            if feat not in input_data:
                                if df_full is not None and feat in df_full.columns:
                                    input_data[feat] = df_full[feat].mode()[0]
                                else:
                                    input_data[feat] = 'Desconocido'
                        
                        return input_data
                    
                    # ═══════════════════════════════════════════════════════
                    # DECISIÓN: ¿TODOS LOS PAÍSES O UNO SOLO?
                    # ═══════════════════════════════════════════════════════
                    
                    if predecir_todos_paises:
                        # ───────────────────────────────────────────────────
                        # PREDICCIÓN PARA TODOS LOS PAÍSES
                        # ───────────────────────────────────────────────────
                        
                        st.markdown("---")
                        st.info("🔄 Calculando predicciones para todos los países...")
                        
                        predicciones_por_pais = []
                        
                        # Determinar qué puntos usar
                        puntos_a_predecir = puntos_disponibles if predecir_todos_puntos else [punto_seleccion]
                        
                        # Progress bar
                        progress_bar = st.progress(0)
                        total_combinaciones = len(paises_disponibles) * len(puntos_a_predecir)
                        contador = 0
                        
                        for pais in paises_disponibles:
                            prediccion_pais_total = 0
                            desglose_puntos = []
                            
                            for punto in puntos_a_predecir:
                                # Crear input
                                input_data = crear_input_data(pais, punto)
                                
                                # Crear DataFrame
                                expected_columns = numeric_features + categorical_features
                                input_df = pd.DataFrame([input_data])
                                input_df = input_df[expected_columns]
                                
                                # Predecir
                                pred = modelo.predict(input_df)[0]
                                prediccion_pais_total += pred
                                
                                desglose_puntos.append({
                                    'punto': punto,
                                    'prediccion': pred
                                })
                                
                                # Actualizar progress
                                contador += 1
                                progress_bar.progress(contador / total_combinaciones)
                            
                            predicciones_por_pais.append({
                                'pais': pais,
                                'prediccion_total': prediccion_pais_total,
                                'desglose': desglose_puntos
                            })
                        
                        progress_bar.empty()
                        
                        # Calcular total general
                        prediccion_total = sum(p['prediccion_total'] for p in predicciones_por_pais)
                        
                        # ───────────────────────────────────────────────────
                        # MOSTRAR RESULTADOS AGREGADOS
                        # ───────────────────────────────────────────────────
                        
                        st.success("✅ Predicción completada para todos los países!")
                        
                        st.markdown("## 🎯 Predicción Total Agregada")
                        
                        col_res1, col_res2, col_res3 = st.columns([2, 1, 1])
                        
                        with col_res1:
                            st.markdown(f"""
                            <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                                        padding: 2rem; border-radius: 15px; text-align: center; color: white;">
                                <h1 style="margin: 0; font-size: 3rem;">{prediccion_total:,.0f}</h1>
                                <p style="margin: 0.5rem 0 0 0; font-size: 1.2rem;">turistas predichos (TOTAL)</p>
                                <p style="margin: 0.2rem 0 0 0; font-size: 0.9rem; opacity: 0.8;">
                                    {mes_nombre} {año_input} • {len(paises_disponibles)} países
                                </p>
                            </div>
                            """, unsafe_allow_html=True)
                        
                        with col_res2:
                            promedio_por_pais = prediccion_total / len(paises_disponibles)
                            st.metric(
                                "Promedio/País",
                                f"{promedio_por_pais:,.0f}",
                                help="Promedio de turistas por país"
                            )
                        
                        with col_res3:
                            if stats:
                                diff_mean = ((prediccion_total - stats['train']['mean']) / stats['train']['mean']) * 100
                                st.metric(
                                    "vs Media",
                                    f"{diff_mean:+.1f}%"
                                )
                        
                        # ───────────────────────────────────────────────────
                        # DESGLOSE POR PAÍS
                        # ───────────────────────────────────────────────────
                        
                        st.markdown("### 🌍 Desglose por País de Origen")
                        
                        # Ordenar por predicción (mayor a menor)
                        predicciones_por_pais.sort(key=lambda x: x['prediccion_total'], reverse=True)
                        
                        # Crear DataFrame para visualización
                        df_paises = pd.DataFrame([
                            {
                                'País': p['pais'],
                                'Turistas Predichos': int(p['prediccion_total']),
                                '% del Total': f"{(p['prediccion_total']/prediccion_total*100):.1f}%"
                            }
                            for p in predicciones_por_pais
                        ])
                        
                        st.dataframe(
                            df_paises,
                            use_container_width=True,
                            hide_index=True
                        )
                        
                        # Gráfico de barras con Altair
                        st.markdown("#### 📊 Visualización")
                        
                        chart_data = pd.DataFrame([
                            {'País': p['pais'], 'Turistas': p['prediccion_total']}
                            for p in predicciones_por_pais
                        ])
                        
                        bar_chart = alt.Chart(chart_data).mark_bar().encode(
                            x=alt.X('Turistas:Q', title='Turistas Predichos'),
                            y=alt.Y('País:N', sort='-x', title='País de Origen'),
                            color=alt.Color('Turistas:Q', scale=alt.Scale(scheme='viridis'), legend=None),
                            tooltip=[
                                alt.Tooltip('País:N', title='País'),
                                alt.Tooltip('Turistas:Q', title='Turistas', format=',')
                            ]
                        ).properties(
                            height=400
                        )
                        
                        st.altair_chart(bar_chart, use_container_width=True)
                        
                        # Top 5 países
                        st.markdown("#### 🏆 Top 5 Países")
                        
                        cols_top = st.columns(5)
                        for i, p in enumerate(predicciones_por_pais[:5]):
                            with cols_top[i]:
                                medalla = ['🥇', '🥈', '🥉', '4️⃣', '5️⃣'][i]
                                st.metric(
                                    f"{medalla} {p['pais']}",
                                    f"{p['prediccion_total']:,.0f}",
                                    delta=f"{(p['prediccion_total']/prediccion_total*100):.1f}%"
                                )
                        
                        # Desglose detallado (opcional)
                        if predecir_todos_puntos and len(puntos_a_predecir) > 1:
                            with st.expander("🔍 Ver desglose por punto de entrada"):
                                for p in predicciones_por_pais[:10]:  # Primeros 10
                                    st.markdown(f"**{p['pais']}** ({p['prediccion_total']:,.0f} total):")
                                    for d in p['desglose']:
                                        st.write(f"   • {d['punto']}: {d['prediccion']:,.0f} turistas")
                    
                    else:
                        # ───────────────────────────────────────────────────
                        # PREDICCIÓN PARA UN PAÍS ESPECÍFICO (CÓDIGO ORIGINAL)
                        # ───────────────────────────────────────────────────
                        
                        puntos_a_predecir = puntos_disponibles if predecir_todos_puntos else [punto_seleccion]
                        
                        prediccion_total = 0
                        desglose_puntos = []
                        
                        for punto in puntos_a_predecir:
                            input_data = crear_input_data(pais_seleccion, punto)
                            
                            expected_columns = numeric_features + categorical_features
                            input_df = pd.DataFrame([input_data])
                            input_df = input_df[expected_columns]
                            
                            pred = modelo.predict(input_df)[0]
                            prediccion_total += pred
                            
                            desglose_puntos.append({
                                'punto': punto,
                                'prediccion': pred
                            })
                        
                        # Mostrar resultado (igual que antes)
                        st.markdown("---")
                        st.success("✅ Predicción realizada exitosamente!")
                        
                        st.markdown("## 🎯 Resultado de la Predicción")
                        
                        col_res1, col_res2, col_res3 = st.columns([2, 1, 1])
                        
                        with col_res1:
                            texto_adicional = f" • {len(puntos_a_predecir)} puntos" if predecir_todos_puntos else ""
                            st.markdown(f"""
                            <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                                        padding: 2rem; border-radius: 15px; text-align: center; color: white;">
                                <h1 style="margin: 0; font-size: 3rem;">{prediccion_total:,.0f}</h1>
                                <p style="margin: 0.5rem 0 0 0; font-size: 1.2rem;">turistas predichos</p>
                                <p style="margin: 0.2rem 0 0 0; font-size: 0.9rem; opacity: 0.8;">
                                    {mes_nombre} {año_input} • {pais_seleccion}{texto_adicional}
                                </p>
                            </div>
                            """, unsafe_allow_html=True)
                        
                        with col_res2:
                            if stats:
                                diff_mean = ((prediccion_total - stats['train']['mean']) / stats['train']['mean']) * 100
                                st.metric(
                                    "vs Media",
                                    f"{diff_mean:+.1f}%",
                                    delta=f"{prediccion_total - stats['train']['mean']:,.0f}"
                                )
                        
                        with col_res3:
                            if stats:
                                percentil = (prediccion_total / stats['train']['max']) * 100
                                st.metric(
                                    "% Máximo",
                                    f"{percentil:.1f}%"
                                )
                        
                        # Desglose por punto (si aplica)
                        if predecir_todos_puntos and len(desglose_puntos) > 1:
                            st.markdown("### 🚪 Desglose por Punto de Entrada")
                            
                            df_puntos = pd.DataFrame([
                                {
                                    'Punto de Entrada': d['punto'],
                                    'Turistas': int(d['prediccion']),
                                    '% del Total': f"{(d['prediccion']/prediccion_total*100):.1f}%"
                                }
                                for d in desglose_puntos
                            ])
                            
                            st.dataframe(df_puntos, use_container_width=True, hide_index=True)
                        
                        # Interpretación (igual que antes)
                        st.markdown("### 📊 Interpretación")
                        
                        if stats:
                            mean = stats['train']['mean']
                            
                            if prediccion_total > mean * 1.5:
                                st.markdown("""
                                <div class="success-box">
                                    🎉 <strong>Demanda Muy Alta</strong>: Excelente período. Maximizar tarifas.
                                </div>
                                """, unsafe_allow_html=True)
                            elif prediccion_total > mean:
                                st.markdown("""
                                <div class="info-box">
                                    📈 <strong>Demanda Por Encima del Promedio</strong>: Buena afluencia esperada.
                                </div>
                                """, unsafe_allow_html=True)
                            elif prediccion_total > mean * 0.7:
                                st.markdown("""
                                <div class="warning-box">
                                    📊 <strong>Demanda Moderada</strong>: Implementar promociones selectivas.
                                </div>
                                """, unsafe_allow_html=True)
                            else:
                                st.markdown("""
                                <div class="warning-box">
                                    ⚠️ <strong>Demanda Baja</strong>: Descuentos y campañas de marketing.
                                </div>
                                """, unsafe_allow_html=True)
                    
                    # ═══════════════════════════════════════════════════════
                    # INCERTIDUMBRE (COMÚN A AMBOS CASOS)
                    # ═══════════════════════════════════════════════════════
                    
                    if metadata:
                        rmse = metadata['metricas']['test_rmse']
                        
                        if predecir_todos_paises:
                            rmse_ajustado = rmse * np.sqrt(len(paises_disponibles))
                            st.markdown(f"""
                            <div class="info-box">
                                📊 <strong>Intervalo de Confianza (aproximado):</strong><br>
                                RMSE ajustado: {rmse_ajustado:,.0f} turistas<br>
                                Rango: <strong>{max(0, prediccion_total - rmse_ajustado):,.0f} - {prediccion_total + rmse_ajustado:,.0f} turistas</strong>
                            </div>
                            """, unsafe_allow_html=True)
                        else:
                            st.markdown(f"""
                            <div class="info-box">
                                📊 <strong>Intervalo de Confianza (~68%):</strong><br>
                                RMSE: {rmse:,.0f} turistas<br>
                                Rango: <strong>{max(0, prediccion_total - rmse):,.0f} - {prediccion_total + rmse:,.0f} turistas</strong>
                            </div>
                            """, unsafe_allow_html=True)
                
                except Exception as e:
                    st.error(f"❌ Error: {str(e)}")
                    with st.expander("🔍 Detalles técnicos"):
                        st.exception(e)
    
    else:
        st.error("❌ Modelo no disponible.")
# ═══════════════════════════════════════════════════════════════════════════
# FOOTER
# ═══════════════════════════════════════════════════════════════════════════
st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #666; padding: 2rem 0;">
    <p><strong>Predictor de Turismo en Argentina</strong> | Desarrollado por Julian Cadenas</p>
    <p>Ciencia de Datos | Entrega Final - Visualización e Integración | 2025</p>
    <p>🏔️ Mendoza, Argentina 🇦🇷</p>
</div>
""", unsafe_allow_html=True)