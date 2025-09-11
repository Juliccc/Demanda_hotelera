# dags/mza_turismo_train.py

from __future__ import annotations

import os
import json
from pathlib import Path
from datetime import datetime

import pandas as pd
import numpy as np
import joblib
import statsmodels.api as sm
from sklearn.metrics import mean_absolute_error
from xgboost import XGBRegressor

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.email import send_email

# ─── Configuración y Variables ─────────────────────────────────────────────────

AIRFLOW_HOME    = Path(os.getenv("AIRFLOW_HOME", "/usr/local/airflow"))
CURATED_PATH    = AIRFLOW_HOME / "data" / "curated" / "mendoza_eoh_curated.parquet"
MODELS_DIR      = AIRFLOW_HOME / "data" / "models"
REGISTRY_FILE   = MODELS_DIR / "registry.json"

# Hyperparámetros XGB y SARIMAX desde Variables de Airflow
XGB_PARAMS      = json.loads(Variable.get("mza_xgb_params", default_var='{"n_estimators":500,"learning_rate":0.05,"max_depth":4,"subsample":0.9,"colsample_bytree":0.9,"random_state":42}'))
SARIMAX_PARAMS  = json.loads(Variable.get("mza_sarimax_params", default_var='{"order":[1,1,1],"seasonal_order":[1,1,1,12],"enforce_stationarity":false,"enforce_invertibility":false}'))

# Parámetros de alerta
ALERT_EMAIL     = Variable.get("turismo_alert_email", default_var="alerts@miempresa.com")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": [ALERT_EMAIL],
    "retries": 1,
    "retry_delay": pd.Timedelta(minutes=5),
}

# ─── Definición del DAG ─────────────────────────────────────────────────────────

with DAG(
    dag_id="mza_turismo_train",
    description="Entrenamiento y selección de modelo de demanda hotelera en Mendoza",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["mendoza", "turismo", "train"],
) as dag:

    @task()
    def load_curated() -> pd.DataFrame:
        """Carga el dataset curado desde parquet y normaliza la fecha."""
        if not CURATED_PATH.exists():
            raise FileNotFoundError(f"Archivo no encontrado: {CURATED_PATH}")
        df = pd.read_parquet(CURATED_PATH)
        df["fecha"] = pd.to_datetime(df["fecha"], format="%Y-%m-%d")
        df = df.sort_values("fecha").reset_index(drop=True)
        return df

    @task()
    def make_features(df: pd.DataFrame) -> pd.DataFrame:
        """Genera variables temporales sen/cos para modelado de estacionalidad."""
        X = df.copy()
        X["mes"]     = X["fecha"].dt.month
        X["anio"]    = X["fecha"].dt.year
        X["sin_m"]   = np.sin(2 * np.pi * X["mes"] / 12)
        X["cos_m"]   = np.cos(2 * np.pi * X["mes"] / 12)
        return X

    @task()
    def train_xgb(X: pd.DataFrame) -> dict:
        """Entrena un XGBoost y retorna metadatos del modelo."""
        MODELS_DIR.mkdir(parents=True, exist_ok=True)
        y      = X["valor"].values
        feats  = ["mes", "anio", "sin_m", "cos_m"]

        model = XGBRegressor(**XGB_PARAMS)
        model.fit(X[feats], y)

        preds = model.predict(X[feats])
        mae   = float(mean_absolute_error(y, preds))

        path  = MODELS_DIR / "xgb_mza.joblib"
        joblib.dump(model, path)

        return {"name": "xgb", "mae": mae, "path": str(path), "feats": feats}

    @task()
    def train_sarimax(X: pd.DataFrame) -> dict:
        """Entrena un SARIMAX y retorna metadatos del modelo."""
        MODELS_DIR.mkdir(parents=True, exist_ok=True)
        ts     = X.set_index("fecha")["valor"]
        params = {k: tuple(v) if isinstance(v, list) else v for k, v in SARIMAX_PARAMS.items()}

        model  = sm.tsa.statespace.SARIMAX(ts, **params)
        res    = model.fit(disp=False)
        preds  = res.predict(start=ts.index[0], end=ts.index[-1])
        mae    = float(mean_absolute_error(ts.values, preds.values))

        path   = MODELS_DIR / "sarimax_mza.joblib"
        joblib.dump(res, path)

        return {"name": "sarimax", "mae": mae, "path": str(path), "feats": ["fecha"]}

    @task(trigger_rule=TriggerRule.ALL_SUCCESS)
    def select_and_register(*models: dict) -> dict:
        """Selecciona el modelo con menor MAE y guarda el registro en JSON."""
        best = min(models, key=lambda m: m["mae"])
        with open(REGISTRY_FILE, "w") as f:
            json.dump(best, f, indent=2)
        return best

    @task(trigger_rule=TriggerRule.ONE_FAILED)
    def notify_failure(context) -> None:
        """Envía un correo si falla alguna tarea de entrenamiento."""
        subject = f"DAG {context['dag'].dag_id} FALLÓ en {context['task_instance_key_str']}"
        html_content = f"""
        <p>DAG: {context['dag'].dag_id}</p>
        <p>Tarea: {context['task_instance_key_str']}</p>
        <p>Fecha de ejecución: {context['ts']}</p>
        <p>Mensaje: Revisa los logs para más detalles.</p>
        """
        send_email(to=ALERT_EMAIL, subject=subject, html_content=html_content)

    # ─── Orquestación ──────────────────────────────────────────────────────────

    df    = load_curated()
    X     = make_features(df)
    m_xgb = train_xgb(X)
    m_sar = train_sarimax(X)
    sel   = select_and_register(m_xgb, m_sar)
    notify_failure()  # Se dispara si alguna de las tareas falla
