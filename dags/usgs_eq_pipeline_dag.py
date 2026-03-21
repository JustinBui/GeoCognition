# ...existing code...
from __future__ import annotations

import io
import json
import os
import logging
from pathlib import Path
import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk.bases.operator import chain
from minio import Minio
import pandas as pd
import numpy as np

from include.common import read_yaml, get_minio_client, debug_context
from include.constants import CONFIG_FILE_PATH, EQ_COLUMNS

cfg = read_yaml(CONFIG_FILE_PATH)
USGS_URL = cfg.data_ingestion.source_URL
MINIO_ENDPOINT = cfg.data_ingestion.minio_endpoint
RAW_BUCKET_NAME = cfg.storage.eq_raw_bucket_name
CURATED_BUCKET_NAME = cfg.storage.eq_curated_bucket_name
logger = logging.getLogger(__name__)


def validate_eq_payload(**context) -> str:
    '''
    Validates the raw JSON payload from the USGS API. Raises ValueError if validation fails.
    Returns the raw JSON text if validation succeeds, which is then pushed to XCom for downstream tasks.
    '''
    raw_json_text = context["ti"].xcom_pull(task_ids="fetch_usgs_events")  # Pulling the raw JSON text from the previous task's XCom
    
    # Check whether JSON has content
    if not raw_json_text:
        raise ValueError("USGS payload is empty")
    
    # Loading JSON if content exists, and validating expected structure
    try:
        payload = json.loads(raw_json_text)
    except json.JSONDecodeError as e:
        raise ValueError(f"USGS payload is not valid JSON: {e}") from e

    # Validating expected keys and types in the payload
    required_keys = ["type", "metadata", "features"]
    missing = [k for k in required_keys if k not in payload]
    if missing:
        raise ValueError(f"USGS payload missing required keys: {missing}")

    # Basic validation of expected types for keys
    if payload.get("type") != "FeatureCollection":
        raise ValueError("USGS payload type is not 'FeatureCollection'")

    # Validate that 'features' is a list (even if empty), as expected from USGS GeoJSON
    if not isinstance(payload.get("features"), list):
        raise ValueError("USGS payload 'features' is not a list")

    logger.info(f"USGS payload validation succeeded with {len(payload['features'])} features")

    # Return validated raw JSON so downstream pulls validated content
    return raw_json_text

def upload_raw_eq_json_to_minio(**context) -> None:
    '''
    Uploads the validated raw JSON payload to MinIO.
    '''
    ds = context["ds"]  # e.g. 2025-01-01
    start_dt = pendulum.parse(ds, tz="UTC")

    raw_json_text = context["ti"].xcom_pull(task_ids="validate_eq_payload") # Pulling the raw JSON text from the previous task's XCom
    if not raw_json_text:
        raise ValueError("Empty response from USGS API")

    object_name = f"year={start_dt:%Y}/month={start_dt:%m}/day={start_dt:%d}/raw.json"
    payload = raw_json_text.encode("utf-8")

    client = get_minio_client()

    if not client.bucket_exists(RAW_BUCKET_NAME):
        client.make_bucket(RAW_BUCKET_NAME)

    client.put_object(
        bucket_name=RAW_BUCKET_NAME,
        object_name=object_name,
        data=io.BytesIO(payload),
        length=len(payload),
        content_type="application/json",
    )

    logger.info(f"Uploaded raw USGS JSON to MinIO at {RAW_BUCKET_NAME}/{object_name}")

def to_list(v) -> list:
    """
    Convert a JSON string to a list, or return the value as-is if it's not a string.
    """
    if isinstance(v, str):
        try:
            return json.loads(v)   # e.g. "[40.1, 9.2, 10.0]" -> [40.1, 9.2, 10.0]
        except Exception:
            return np.nan
    return v

def flatten_eq_json_to_df(**context) -> pd.DataFrame:
    '''
    Flattens the raw JSON payload and returns a DataFrame
    '''
    ds = context["ds"]  # e.g. 2025-01-01
    start_dt = pendulum.parse(ds, tz="UTC")

    raw_object_path = f"year={start_dt:%Y}/month={start_dt:%m}/day={start_dt:%d}/raw.json"
    
    client = get_minio_client()
    if not client.bucket_exists(CURATED_BUCKET_NAME):
        client.make_bucket(CURATED_BUCKET_NAME)

    # Pulling JSON from MinIO
    response = client.get_object(RAW_BUCKET_NAME, raw_object_path)
    try:
        payload = json.loads(response.read().decode("utf-8"))
    finally:
        # Always release network resources, even if JSON parsing fails.
        response.close() # Closing the response stream
        response.release_conn() # Return the HTTP connection to the pool, making the connection available for the next request

    # Flattening JSON to DataFrame
    features = payload.get("features", [])
    if not features:
        logger.info("No USGS features for %s; writing empty curated parquet.", ds)
        return pd.DataFrame(columns=EQ_COLUMNS)

    df_features = pd.json_normalize(features)

    # Handle geometry.coordinates when present: [longitude, latitude, depth]
    if "geometry.coordinates" in df_features.columns:
        coords = df_features["geometry.coordinates"].apply(lambda x: to_list(x) if pd.notna(x) else x)
        df_features["longitude"] = coords.str[0]
        df_features["latitude"] = coords.str[1]
        df_features["depth_km"] = coords.str[2]
        df_features.drop(columns=["geometry.coordinates"], inplace=True)
    else:
        logger.info("No geometry.coordinates found; filling longitude, latitude, and depth_km with NaN")
        df_features["longitude"] = np.nan
        df_features["latitude"] = np.nan
        df_features["depth_km"] = np.nan

    # Ensure all expected columns exist, then enforce output column order.
    for col in EQ_COLUMNS:
        if col not in df_features.columns:
            logger.info(f"Column '{col}' missing from flattened DataFrame; filling with NaN")
            df_features[col] = np.nan
    df_features = df_features[EQ_COLUMNS]
    
    logger.info(f"Flattened USGS JSON to DataFrame with {len(df_features)} rows and columns: {df_features.columns.tolist()}")
    
    return df_features

def upload_flattened_eq_to_minio(**context) -> None:
    """
    Uploads the flattened DataFrame as Parquet to MinIO
    """
    ds = context["ds"]
    start_dt = pendulum.parse(ds, tz="UTC")
    parquet_object_path = f"year={start_dt:%Y}/month={start_dt:%m}/day={start_dt:%d}/flattened.parquet"

    client = get_minio_client()
    if not client.bucket_exists(CURATED_BUCKET_NAME):
        client.make_bucket(CURATED_BUCKET_NAME)

    # Converting DataFrame to Parquet in-memory, then uploading to MinIO
    buf = io.BytesIO()
    df_features = context["ti"].xcom_pull(task_ids="flatten_eq_json_to_df")  # Pulling the flattened DataFrame from the previous task's XCom
    df_features.to_parquet(buf, index=False, engine="pyarrow")
    data = buf.getvalue()

    client.put_object(
        bucket_name=CURATED_BUCKET_NAME,
        object_name=parquet_object_path,
        data=io.BytesIO(data),
        length=len(data),
        content_type="application/octet-stream",
    )
    logger.info(f"Uploaded flattened USGS DataFrame to MinIO at {CURATED_BUCKET_NAME}/{parquet_object_path}")

# def load_parquet_to_postgres(**context) -> None:
#     """
#     Reads curated parquet from MinIO and upserts into Postgres for app serving.
#     """
#     ds = context["ds"]
#     start_dt = pendulum.parse(ds, tz="UTC")
#     parquet_object_path = f"year={start_dt:%Y}/month={start_dt:%m}/day={start_dt:%d}/flattened.parquet"

#     client = get_minio_client()
#     response = client.get_object(CURATED_BUCKET_NAME, parquet_object_path)
#     try:
#         parquet_bytes = response.read()
#     finally:
#         response.close()
#         response.release_conn()
    
#     df = pd.read_parquet(io.BytesIO(parquet_bytes), engine="pyarrow")
#     if df.empty:
#         return  # No data to load for this day

#     # Ensure all expected source columns exist and preserve order
#     for col in EQ_COLUMNS:
#         if col not in df.columns:
#             df[col] = pd.NA
#     df = df[EQ_COLUMNS]
    
#     # Convert nested list/dict to JSON text for postgres jsonb column
#     df["geometry.coordinates"] = df["geometry.coordinates"].apply(
#         lambda v: json.dumps(v) if isinstance(v, (list, dict)) else (None if pd.isna(v) else str(v))
#     )


with DAG(
    dag_id="usgs_to_minio_daily_http_operator",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="@daily", # run at the top of every day, pulling yesterday's data at 00:00 UTC
    catchup=False,
    max_active_runs=1,
    tags=["usgs", "minio", "raw"]
) as dag:
    fetch_usgs_events_task = HttpOperator(
        task_id="fetch_usgs_events",
        http_conn_id="usgs_api",  # create this connection in Airflow
        method="GET",
        endpoint="fdsnws/event/1/query",
        data={
            "format": "geojson",
            "starttime": "{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S') }}",
            "endtime": "{{ (data_interval_start + macros.timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%S') }}",
            "minmagnitude": "2.5",
            "orderby": "time-asc",
            "limit": "20000",
        },
        response_check=lambda response: response.status_code == 200, # Raise AirflowException if not 200
        response_filter=lambda response: response.text,  # Pushed to XCom
        log_response=False, # Avoid logging large JSON payloads in Airflow logs
    )

    validate_eq_payload_task = PythonOperator(
        task_id="validate_eq_payload",
        python_callable=validate_eq_payload,
    )
    
    upload_raw_eq_json_to_minio_task = PythonOperator(
        task_id="upload_raw_eq_json_to_minio",
        python_callable=upload_raw_eq_json_to_minio,
    )

    flatten_eq_json_to_df_task = PythonOperator(
        task_id="flatten_eq_json_to_df",
        python_callable=flatten_eq_json_to_df,
    )

    upload_flattened_eq_to_minio_task = PythonOperator(
        task_id="upload_flattened_eq_to_minio",
        python_callable=upload_flattened_eq_to_minio,
    )

    # DAG structure
    chain(
        fetch_usgs_events_task,
        validate_eq_payload_task,
        upload_raw_eq_json_to_minio_task,
        flatten_eq_json_to_df_task,
        upload_flattened_eq_to_minio_task
    )
