# ...existing code...
from __future__ import annotations

import io
import json
import os

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.http.operators.http import HttpOperator
from minio import Minio
import pandas as pd

from include.helpers import read_yaml, get_minio_client, debug_context
from include.constants import CONFIG_FILE_PATH, EQ_COLUMNS

cfg = read_yaml(CONFIG_FILE_PATH)
USGS_URL = cfg.data_ingestion.source_URL
MINIO_ENDPOINT = cfg.data_ingestion.minio_endpoint
RAW_BUCKET_NAME = cfg.storage.eq_raw_bucket_name
CURATED_BUCKET_NAME = cfg.storage.eq_curated_bucket_name

def validate_usgs_eq_payload(**context) -> str:
    '''
    Validates the raw JSON payload from the USGS API. Raises ValueError if validation fails.
    Returns the raw JSON text if validation succeeds, which is then pushed to XCom for downstream tasks.
    '''
    raw_json_text = context["ti"].xcom_pull(task_ids="fetch_usgs_events")  # Pulling the raw JSON text from the previous task's XCom
    if not raw_json_text:
        raise ValueError("USGS payload is empty")

    try:
        payload = json.loads(raw_json_text)
    except json.JSONDecodeError as e:
        raise ValueError(f"USGS payload is not valid JSON: {e}") from e

    required_keys = ["type", "metadata", "features"]
    missing = [k for k in required_keys if k not in payload]
    if missing:
        raise ValueError(f"USGS payload missing required keys: {missing}")

    if payload.get("type") != "FeatureCollection":
        raise ValueError("USGS payload type is not 'FeatureCollection'")

    if not isinstance(payload.get("features"), list):
        raise ValueError("USGS payload 'features' is not a list")

    # Return validated raw JSON so downstream pulls validated content
    return raw_json_text

def upload_raw_eq_json_to_minio(**context) -> None:
    '''
    Uploads the validated raw JSON payload to MinIO.
    '''
    ds = context["ds"]  # e.g. 2025-01-01
    start_dt = pendulum.parse(ds, tz="UTC")

    raw_json_text = context["ti"].xcom_pull(task_ids="validate_usgs_eq_payload") # Pulling the raw JSON text from the previous task's XCom
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

def flatten_eq_json_to_parquet(**context) -> None:
    '''
    Flattens the raw JSON payload and saves it as Parquet in MinIO
    '''
    ds = context["ds"]  # e.g. 2025-01-01
    start_dt = pendulum.parse(ds, tz="UTC")

    raw_object_path = f"year={start_dt:%Y}/month={start_dt:%m}/day={start_dt:%d}/raw.json"
    parquet_object_path = f"year={start_dt:%Y}/month={start_dt:%m}/day={start_dt:%d}/flattened.parquet"
    
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
    df_features = pd.json_normalize(features)

    if df_features.empty:
        # No events for this day: still emit empty parquet with expected schema.
        df_features = pd.DataFrame(columns=EQ_COLUMNS)
    else:
        # Ensure all expected columns exist, then enforce output column order.
        for col in EQ_COLUMNS:
            if col not in df_features.columns:
                df_features[col] = pd.NA
        df_features = df_features[EQ_COLUMNS]
    
    # Converting DataFrame to Parquet in-memory, then uploading to MinIO
    buf = io.BytesIO()
    df_features.to_parquet(buf, index=False, engine="pyarrow")
    data = buf.getvalue()

    client.put_object(
        bucket_name=CURATED_BUCKET_NAME,
        object_name=parquet_object_path,
        data=io.BytesIO(data),
        length=len(data),
        content_type="application/octet-stream",
    )

with DAG(
    dag_id="usgs_to_minio_daily_http_operator",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    tags=["usgs", "minio", "raw"],
) as dag:
    fetch_usgs_events = HttpOperator(
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
        response_check=lambda response: response.status_code == 200,
        response_filter=lambda response: response.text,  # pushed to XCom
        log_response=False,
    )

    validate_payload = PythonOperator(
        task_id="validate_usgs_eq_payload",
        python_callable=validate_usgs_eq_payload,
    )
    
    upload_to_minio = PythonOperator(
        task_id="upload_raw_eq_json_to_minio",
        python_callable=upload_raw_eq_json_to_minio,
    )

    flatten_to_parquet = PythonOperator(
        task_id="flatten_eq_json_to_parquet",
        python_callable=flatten_eq_json_to_parquet,
    )

    fetch_usgs_events >> validate_payload >> upload_to_minio >> flatten_to_parquet
# ...existing code...
