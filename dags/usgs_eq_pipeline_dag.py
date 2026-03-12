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

from include.helpers import read_yaml, get_minio_client
from include.constants import *

USGS_URL = read_yaml(CONFIG_FILE_PATH).data_ingestion.source_URL
BUCKET_NAME = read_yaml(CONFIG_FILE_PATH).data_ingestion.earthquake_bucket_name

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

    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)

    client.put_object(
        bucket_name=BUCKET_NAME,
        object_name=object_name,
        data=io.BytesIO(payload),
        length=len(payload),
        content_type="application/json",
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

    fetch_usgs_events >> validate_payload >> upload_to_minio
# ...existing code...