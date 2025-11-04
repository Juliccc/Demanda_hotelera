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
    page_title="Predictor Turismo Mendoza",
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
        background-color: #fff3cd;
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
    """Carga el dataset completo"""
    try:
        df = pd.read_csv('mendoza_turismo_final_filtrado.csv', sep=';', encoding='utf-8')
        # Eliminar registros con 0 turistas (coherente con el notebook)
        df = df[df['turistas'] > 0].copy()
        return df
    except FileNotFoundError:
        st.warning("⚠️ No se encontró el dataset completo. Usando datos de muestra.")
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
st.markdown('<div class="main-header">🏔️ Predictor de Turismo en Mendoza</div>', unsafe_allow_html=True)

st.markdown("""
<div class="info-box">
    📊 <strong>Aplicación de Machine Learning</strong> para predecir la demanda turística mensual en Mendoza, Argentina.
    Basada en datos históricos de 2014-2025 y modelos de regresión entrenados con Scikit-learn.
</div>
""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════════════
# SIDEBAR - NAVEGACIÓN
# ═══════════════════════════════════════════════════════════════════════════
st.sidebar.image("https://upload.wikimedia.org/wikipedia/commons/thumb/1/14/Bandera_de_Mendoza.svg/320px-Bandera_de_Mendoza.svg.png", width=200)
st.sidebar.title("📂 Navegación")
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
        para predecir la demanda turística mensual en Mendoza.
        
        **Objetivo:**
        - Predecir el número de turistas que visitarán Mendoza en un mes específico
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
                    st.metric("Países de Origen", df_full['pais_origen'].nunique())
    
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
            with col3:
                st.metric("Valores Nulos", df_full.isnull().sum().sum())
        
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
        # VISUALIZACIÓN 2: ESTACIONALIDAD
        # ═══════════════════════════════════════════════════════════════════
        st.subheader("📊 2. Estacionalidad Mensual")
        
        # Agregar por mes (promedio across años)
        df_mensual = df_full.groupby('mes').agg({'turistas': 'mean'}).reset_index()
        df_mensual['mes_nombre'] = df_mensual['mes'].map({
            1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril',
            5: 'Mayo', 6: 'Junio', 7: 'Julio', 8: 'Agosto',
            9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'
        })
        
        bar_chart = alt.Chart(df_mensual).mark_bar(
            cornerRadiusTopLeft=5,
            cornerRadiusTopRight=5
        ).encode(
            x=alt.X('mes_nombre:N', title='Mes', sort=[
                'Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio',
                'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre'
            ]),
            y=alt.Y('turistas:Q', title='Promedio de Turistas', axis=alt.Axis(format=',')),
            color=alt.Color('turistas:Q', scale=alt.Scale(scheme='blues'), legend=None),
            tooltip=[
                alt.Tooltip('mes_nombre:N', title='Mes'),
                alt.Tooltip('turistas:Q', title='Promedio Turistas', format=',')
            ]
        ).properties(
            width=800,
            height=400,
            title='Promedio de Turistas por Mes (Estacionalidad)'
        )
        
        st.altair_chart(bar_chart, use_container_width=True)
        
        st.markdown("""
        **💡 Insights:**
        - 📅 Identificación de temporada alta y baja
        - 🌞 Patrones estacionales claros
        - 📊 Útil para planificación de recursos
        """)
        
        st.markdown("---")
        
        # ═══════════════════════════════════════════════════════════════════
        # VISUALIZACIÓN 3: TOP PAÍSES
        # ═══════════════════════════════════════════════════════════════════
        if 'pais_origen' in df_full.columns:
            st.subheader("🌍 3. Principales Países de Origen")
            
            top_paises = df_full.groupby('pais_origen').agg({'turistas': 'sum'}).reset_index()
            top_paises = top_paises.nlargest(10, 'turistas')
            
            # Selector interactivo
            selection = alt.selection_point(fields=['pais_origen'], on='mouseover')
            
            paises_chart = alt.Chart(top_paises).mark_bar().encode(
                x=alt.X('turistas:Q', title='Total de Turistas', axis=alt.Axis(format=',')),
                y=alt.Y('pais_origen:N', title='País de Origen', sort='-x'),
                color=alt.condition(
                    selection,
                    alt.Color('turistas:Q', scale=alt.Scale(scheme='viridis'), legend=None),
                    alt.value('lightgray')
                ),
                tooltip=[
                    alt.Tooltip('pais_origen:N', title='País'),
                    alt.Tooltip('turistas:Q', title='Total Turistas', format=',')
                ]
            ).add_params(
                selection
            ).properties(
                width=800,
                height=400,
                title='Top 10 Países de Origen de Turistas'
            )
            
            st.altair_chart(paises_chart, use_container_width=True)
            
            st.markdown("""
            **💡 Insights:**
            - 🌎 Principales mercados turísticos identificados
            - 📊 Base para estrategias de marketing focalizadas
            - 🎯 Oportunidades de crecimiento por mercado
            """)
    
    else:
        st.warning("⚠️ Datos no disponibles para generar visualizaciones.")

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
                "Features": metadata['dataset_info']['n_features'],
                "Fecha Corte Train/Test": metadata['dataset_info']['fecha_corte_train_test'],
                "Registros Eliminados (0 turistas)": metadata['dataset_info']['registros_eliminados_ceros']
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
        📝 Ingresa los valores de las variables para predecir el número de turistas.
        El modelo procesará automáticamente los datos y generará una predicción.
    </div>
    """, unsafe_allow_html=True)
    
    if modelo and metadata:
        # Formulario de entrada
        st.subheader("📝 Ingresa los Datos")
        
        with st.form("form_prediccion"):
            # Dividir en columnas
            col1, col2, col3 = st.columns(3)
            
            # Obtener features del metadata
            numeric_features = metadata['features']['numericas']
            categorical_features = metadata['features']['categoricas']
            
            # Diccionario para almacenar inputs
            input_data = {}
            
            # INPUTS NUMÉRICOS
            with col1:
                st.markdown("### 🔢 Variables Numéricas")
                
                # Año
                if 'año' in numeric_features:
                    input_data['año'] = st.number_input(
                        "Año",
                        min_value=2024,
                        max_value=2030,
                        value=2025,
                        step=1
                    )
                
                # Mes
                if 'mes' in numeric_features:
                    input_data['mes'] = st.selectbox(
                        "Mes",
                        options=list(range(1, 13)),
                        format_func=lambda x: [
                            'Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio',
                            'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre'
                        ][x-1]
                    )
                
                # Precio USD
                if 'precio_promedio_usd' in numeric_features:
                    input_data['precio_promedio_usd'] = st.number_input(
                        "Precio Promedio (USD)",
                        min_value=0.0,
                        max_value=5000.0,
                        value=850.0,
                        step=50.0
                    )
                
                # Otras variables numéricas (valores por defecto)
                for feat in numeric_features:
                    if feat not in ['año', 'mes', 'precio_promedio_usd', 'turistas']:
                        if 'usd' in feat.lower() or 'precio' in feat.lower():
                            input_data[feat] = st.number_input(
                                feat.replace('_', ' ').title(),
                                value=850.0,
                                step=10.0,
                                key=feat
                            )
                        elif 'tasa' in feat.lower() or 'variacion' in feat.lower():
                            input_data[feat] = st.number_input(
                                feat.replace('_', ' ').title(),
                                value=0.0,
                                step=0.1,
                                key=feat
                            )
                        else:
                            input_data[feat] = st.number_input(
                                feat.replace('_', ' ').title(),
                                value=0.0,
                                step=1.0,
                                key=feat
                            )
            
            # INPUTS CATEGÓRICOS
            with col2:
                st.markdown("### 📋 Variables Categóricas")
                
                # País de origen
                if 'pais_origen' in categorical_features:
                    paises_disponibles = []
                    if df_full is not None and 'pais_origen' in df_full.columns:
                        paises_disponibles = sorted(df_full['pais_origen'].unique().tolist())
                    else:
                        paises_disponibles = ['Chile', 'Brasil', 'Estados Unidos', 'Uruguay', 'Paraguay']
                    
                    input_data['pais_origen'] = st.selectbox(
                        "País de Origen",
                        options=paises_disponibles
                    )
                
                # Punto de entrada
                if 'punto_entrada' in categorical_features:
                    puntos_disponibles = []
                    if df_full is not None and 'punto_entrada' in df_full.columns:
                        puntos_disponibles = sorted(df_full['punto_entrada'].unique().tolist())
                    else:
                        puntos_disponibles = ['Aeropuerto Buenos Aires', 'Paso Cristo Redentor', 'Aeropuerto Mendoza']
                    
                    input_data['punto_entrada'] = st.selectbox(
                        "Punto de Entrada",
                        options=puntos_disponibles
                    )
                
                # Otras variables categóricas
                for feat in categorical_features:
                    if feat not in ['pais_origen', 'punto_entrada']:
                        if df_full is not None and feat in df_full.columns:
                            opciones = sorted(df_full[feat].unique().tolist())
                        else:
                            opciones = ['Opción 1', 'Opción 2', 'Opción 3']
                        
                        input_data[feat] = st.selectbox(
                            feat.replace('_', ' ').title(),
                            options=opciones,
                            key=feat
                        )
            
            # INFORMACIÓN ADICIONAL
            with col3:
                st.markdown("### ℹ️ Información")
                st.markdown("""
                **💡 Tips para predicciones:**
                
                - Selecciona el mes y año deseado
                - Ajusta el precio según mercado
                - Elige el país de origen principal
                - Selecciona el punto de entrada
                
                **📊 El modelo considera:**
                - Estacionalidad mensual
                - Tendencias históricas
                - Variables económicas
                - Origen de turistas
                """)
                
                st.markdown("---")
                
                if stats:
                    st.markdown("### 📈 Estadísticas Históricas")
                    st.metric("Media Histórica", f"{stats['train']['mean']:,.0f} turistas")
                    st.metric("Mediana Histórica", f"{stats['train']['median']:,.0f} turistas")
                    st.metric("Máximo Histórico", f"{stats['train']['max']:,.0f} turistas")
            
            # Botón de predicción
            submitted = st.form_submit_button("🔮 Realizar Predicción", use_container_width=True)
            
            if submitted:
                try:
                    # Crear DataFrame con los inputs
                    input_df = pd.DataFrame([input_data])
                    
                    # Asegurar orden correcto de columnas
                    expected_columns = numeric_features + categorical_features
                    input_df = input_df[expected_columns]
                    
                    # Realizar predicción
                    prediccion = modelo.predict(input_df)[0]
                    
                    # Mostrar resultado
                    st.markdown("---")
                    st.success("✅ Predicción realizada exitosamente!")
                    
                    # Resultado principal
                    st.markdown("## 🎯 Resultado de la Predicción")
                    
                    col_res1, col_res2, col_res3 = st.columns([2, 1, 1])
                    
                    with col_res1:
                        st.markdown(f"""
                        <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                                    padding: 2rem; border-radius: 15px; text-align: center; color: white;">
                            <h1 style="margin: 0; font-size: 3rem;">{prediccion:,.0f}</h1>
                            <p style="margin: 0.5rem 0 0 0; font-size: 1.2rem;">turistas predichos</p>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    with col_res2:
                        if stats:
                            diff_mean = ((prediccion - stats['train']['mean']) / stats['train']['mean']) * 100
                            st.metric(
                                "vs Media Histórica",
                                f"{diff_mean:+.1f}%",
                                delta=f"{prediccion - stats['train']['mean']:,.0f}"
                            )
                    
                    with col_res3:
                        if stats:
                            percentil = (prediccion / stats['train']['max']) * 100
                            st.metric(
                                "% del Máximo",
                                f"{percentil:.1f}%"
                            )
                    
                    # Interpretación
                    st.markdown("### 📊 Interpretación")
                    
                    if stats:
                        mean = stats['train']['mean']
                        
                        if prediccion > mean * 1.5:
                            st.markdown("""
                            <div class="success-box">
                                🎉 <strong>Demanda Alta</strong>: La predicción indica una demanda muy superior 
                                al promedio histórico. Excelente período para maximizar ocupación y precios.
                            </div>
                            """, unsafe_allow_html=True)
                        elif prediccion > mean:
                            st.markdown("""
                            <div class="info-box">
                                📈 <strong>Demanda Por Encima del Promedio</strong>: Se espera una buena afluencia 
                                de turistas. Período favorable para el sector.
                            </div>
                            """, unsafe_allow_html=True)
                        elif prediccion > mean * 0.7:
                            st.markdown("""
                            <div class="warning-box">
                                📊 <strong>Demanda Moderada</strong>: Afluencia cercana al promedio. 
                                Considerar estrategias para aumentar ocupación.
                            </div>
                            """, unsafe_allow_html=True)
                        else:
                            st.markdown("""
                            <div class="warning-box">
                                ⚠️ <strong>Demanda Baja</strong>: Predicción por debajo del promedio. 
                                Recomendable implementar promociones y descuentos.
                            </div>
                            """, unsafe_allow_html=True)
                    
                    # Mostrar datos ingresados
                    with st.expander("📋 Ver datos ingresados"):
                        st.dataframe(input_df, use_container_width=True)
                    
                    # Advertencia sobre incertidumbre
                    if metadata:
                        rmse = metadata['metricas']['test_rmse']
                        st.markdown(f"""
                        <div class="info-box">
                            📊 <strong>Incertidumbre de la predicción:</strong> El modelo tiene un RMSE de {rmse:,.0f} turistas.
                            Esto significa que la predicción real podría estar en el rango de 
                            <strong>{prediccion - rmse:,.0f} a {prediccion + rmse:,.0f} turistas</strong> 
                            con aproximadamente 68% de confianza.
                        </div>
                        """, unsafe_allow_html=True)
                
                except Exception as e:
                    st.error(f"❌ Error al realizar la predicción: {str(e)}")
                    st.exception(e)
    
    else:
        st.error("❌ Modelo no disponible. Por favor verifica que el archivo del modelo exista.")

# ═══════════════════════════════════════════════════════════════════════════
# FOOTER
# ═══════════════════════════════════════════════════════════════════════════
st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #666; padding: 2rem 0;">
    <p><strong>Predictor de Turismo en Mendoza</strong> | Desarrollado por Julian Cadenas</p>
    <p>Ciencia de Datos | Entrega Final - Visualización e Integración | 2025</p>
    <p>🏔️ Mendoza, Argentina 🇦🇷</p>
</div>
""", unsafe_allow_html=True)