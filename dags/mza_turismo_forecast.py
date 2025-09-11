# dags/mza_turismo_forecast.py

from __future__ import annotations
import os
import json
import logging

from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np
import joblib

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

# ─── Configuración base ────────────────────────────────────────────────────────

AIRFLOW_HOME    = Path(os.getenv("AIRFLOW_HOME", "/usr/local/airflow"))
DATA_ROOT       = AIRFLOW_HOME / "data"
CURATED_ROOT    = DATA_ROOT / "curated"
MODELS_ROOT     = DATA_ROOT / "models"
REGISTRY_PATH   = MODELS_ROOT / "registry.json"

DEFAULT_HORIZON = int(Variable.get("turismo_forecast_horizon", 12))
ALERT_EMAIL     = Variable.get("turismo_alert_email", "alerts@miempresa.com")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": [ALERT_EMAIL],
    "retries": 2,
    "retry_delay": pd.to_timedelta(5, unit="m"),
}

# ─── Definición del DAG ────────────────────────────────────────────────────────

with DAG(
    dag_id="mza_turismo_forecast",
    default_args=default_args,
    description="Pronóstico de demanda hotelera en Mendoza: carga de modelo, forecast y persistencia",
    schedule="0 7 10 * *",  # <-- Cambia aquí
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["mendoza", "turismo", "forecast"],
) as dag:

    @task()
    def load_registry() -> dict:
        """Lee el JSON de registro del mejor modelo."""
        with open(REGISTRY_PATH, "r") as f:
            registry = json.load(f)
        logging.info("Modelo seleccionado: %s", registry["name"])
        return registry

    @task()
    def load_historical(ds: str) -> pd.DataFrame:
        """
        Carga el dataset curado correspondiente a la fecha de ejecución.
        """
        path = CURATED_ROOT / ds / "mza_turismo_curated.parquet"
        df = pd.read_parquet(path)
        df["fecha"] = pd.to_datetime(df["fecha"])
        df = df.sort_values("fecha")
        return df

    @task()
    def make_future(df: pd.DataFrame, horizon: int) -> pd.DataFrame:
        """
        Construye el DataFrame de fechas futuras y calcula features.
        """
        last_date = df["fecha"].max()
        future_idx = pd.date_range(last_date, periods=horizon + 1, freq="M")[1:]
        fut = pd.DataFrame({"fecha": future_idx})
        fut["mes"]   = fut["fecha"].dt.month
        fut["anio"]  = fut["fecha"].dt.year
        fut["sin_m"] = np.sin(2 * np.pi * fut["mes"] / 12)
        fut["cos_m"] = np.cos(2 * np.pi * fut["mes"] / 12)
        return fut

    @task()
    def forecast_predictions(registry: dict, future: pd.DataFrame) -> pd.DataFrame:
        """
        Carga el modelo, genera predicciones y anexa metadatos.
        """
        model_path = MODELS_ROOT / registry["path"]
        model = joblib.load(model_path)
        feats = registry["feats"]

        preds = model.predict(future[feats])
        future = future.copy()
        future["prediccion"] = preds
        future["modelo"]     = registry["name"]
        future["mae"]        = registry.get("mae", None)
        return future

    @task(trigger_rule=TriggerRule.ALL_SUCCESS)
    def save_forecast(forecast_df: pd.DataFrame, ds: str) -> str:
        """
        Guarda el DataFrame de predicciones en Parquet, particionado por fecha de ejecución.
        """
        out_folder = CURATED_ROOT / ds
        out_folder.mkdir(parents=True, exist_ok=True)

        out_path = out_folder / f"mza_turismo_forecast_{ds}.parquet"
        forecast_df.to_parquet(out_path, index=False)
        logging.info("Predicciones guardadas en %s", out_path)
        return str(out_path)

    # ─── Orquestación ──────────────────────────────────────────────────────────

    registry      = load_registry()
    historical    = load_historical(ds="{{ ds }}")
    future_df     = make_future(historical, DEFAULT_HORIZON)
    preds         = forecast_predictions(registry, future_df)
    saved_path    = save_forecast(preds, ds="{{ ds }}")