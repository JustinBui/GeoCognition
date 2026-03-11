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

from include.helpers import read_yaml
from include.constants import *

USGS_URL = read_yaml(CONFIG_FILE_PATH).data_ingestion.source_URL
BUCKET_NAME = read_yaml(CONFIG_FILE_PATH).data_ingestion.earthquake_bucket_name


def get_minio_client() -> Minio:
    endpoint = read_yaml(CONFIG_FILE_PATH).data_ingestion.minio_endpoint
    access_key = os.getenv("MINIO_ACCESS_KEY", "minio")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minio123")

    return Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False,
    )


def upload_raw_json_to_minio(**context) -> None:
    ds = context["ds"]  # e.g. 2025-01-01
    start_dt = pendulum.parse(ds, tz="UTC")

    raw_json_text = context["ti"].xcom_pull(task_ids="fetch_usgs_events") # Pulling the raw JSON text from the previous task's XCom
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
            "endtime": "{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S') }}",
            "minmagnitude": "2.5",
            "orderby": "time-asc",
            "limit": "20000",
        },
        response_check=lambda response: response.status_code == 200,
        response_filter=lambda response: response.text,  # pushed to XCom
        log_response=False,
    )

    upload_to_minio = PythonOperator(
        task_id="upload_raw_json_to_minio",
        python_callable=upload_raw_json_to_minio,
    )

    fetch_usgs_events >> upload_to_minio
# ...existing code...