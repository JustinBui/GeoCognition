import io
import json
import logging
import pendulum
from airflow import DAG
from airflow.sdk import task, get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.operators.http import HttpOperator
from airflow.sdk.bases.operator import chain
import pandas as pd
from include.common import (
    read_yaml,
    get_minio_client,
    get_partition_path,
    dataframe_to_parquet_bytes,
)
from include.constants import CONFIG_FILE_PATH, EQ_COLUMNS_ORIGINAL
from include.usgs_eq_helper import (
    validate_eq_payload_helper,
    flatten_eq_json_to_df_helper,
    create_postgis_table_helper,
)

cfg = read_yaml(CONFIG_FILE_PATH)
RAW_BUCKET_NAME = cfg.storage.eq_raw_bucket_name
CURATED_BUCKET_NAME = cfg.storage.eq_curated_bucket_name
POSTGRES_CONN_ID = cfg.postgres.conn_id
logger = logging.getLogger(__name__)

# Maps EQ_COLUMNS_ORIGINAL dot-notation names → valid Postgres column names.
# Top-level GeoJSON "type" ("Feature") and properties.type ("earthquake") would
# collide if both were just called "type", so they get distinct names.
_PG_COL_RENAME = {
    "type": "feature_type",
    "properties.type": "event_type",
    "properties.magType": "mag_type",
}
for _col in EQ_COLUMNS_ORIGINAL:
    if _col not in _PG_COL_RENAME:
        _PG_COL_RENAME[_col] = _col.replace("properties.", "")


@task(task_id="validate_eq_payload")
def validate_eq_payload(raw_json_text: str) -> str:
    """
    Validates the raw JSON payload from the USGS API. Raises ValueError if validation fails.
    Returns the raw JSON text if validation succeeds, which is then pushed to XCom for downstream tasks.
    """
    payload = validate_eq_payload_helper(raw_json_text)  # Validate the payload
    logger.info(
        f"USGS payload validation succeeded with {len(payload['features'])} features"
    )
    return raw_json_text


@task(task_id="upload_raw_eq_json_to_minio")
def upload_raw_eq_json_to_minio(raw_json_text: str) -> str:
    """
    Uploads the validated raw JSON payload to MinIO.
    """
    context = get_current_context()
    ds = context["ds"]  # e.g. 2025-01-01
    raw_object_path = get_partition_path(
        ds, "raw.json"
    )  # e.g. year=2025/month=01/day=01/raw.json
    payload = raw_json_text.encode("utf-8")

    client = get_minio_client()

    if not client.bucket_exists(RAW_BUCKET_NAME):
        client.make_bucket(RAW_BUCKET_NAME)

    client.put_object(
        bucket_name=RAW_BUCKET_NAME,
        object_name=raw_object_path,
        data=io.BytesIO(payload),
        length=len(payload),
        content_type="application/json",
    )

    logger.info(
        f"Uploaded raw USGS JSON to MinIO at {RAW_BUCKET_NAME}/{raw_object_path}"
    )
    return raw_object_path


@task(task_id="flatten_eq_json_to_df")
def flatten_eq_json_to_df(raw_object_path: str) -> pd.DataFrame:
    """
    Flattens the raw JSON payload and returns a DataFrame
    """
    client = get_minio_client()
    response = client.get_object(
        RAW_BUCKET_NAME, raw_object_path
    )  # Pulling JSON from MinIO

    try:
        payload = json.loads(response.read().decode("utf-8"))
    finally:
        # Always release network resources, even if JSON parsing fails.
        response.close()  # Closing the response stream
        response.release_conn()  # Return the HTTP connection to the pool, making the connection available for the next request

    df = flatten_eq_json_to_df_helper(
        payload, EQ_COLUMNS_ORIGINAL
    )  # Flatten the JSON to a DataFrame using the helper function

    logger.info(
        f"Flattened USGS JSON to DataFrame with {len(df)} rows and columns: {df.columns.tolist()}"
    )

    return df


@task(task_id="rename_df_columns")
def rename_df_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renames DataFrame columns from dot notation to SQL-safe names.
    """
    df.drop(
        "type", axis=1, inplace=True
    )  # Drop irrelevant top-level GeoJSON "type" column which is always "Feature"
    df.rename(columns={"properties.type": "seismic_event"}, inplace=True)
    df.rename(columns={"properties.types": "product_types"}, inplace=True)
    df.rename(columns=lambda c: c.replace("properties.", ""), inplace=True)

    logger.info(f"Renamed DataFrame columns to SQL-safe names: {df.columns.tolist()}")
    return df


@task(task_id="upload_flattened_eq_to_minio")
def upload_flattened_eq_to_minio(df: pd.DataFrame) -> str:
    """
    Uploads the flattened DataFrame as Parquet to MinIO
    """
    context = get_current_context()
    ds = context["ds"]
    parquet_object_path = get_partition_path(ds, "flattened.parquet")

    client = get_minio_client()
    if not client.bucket_exists(CURATED_BUCKET_NAME):
        client.make_bucket(CURATED_BUCKET_NAME)

    data = dataframe_to_parquet_bytes(df)

    client.put_object(
        bucket_name=CURATED_BUCKET_NAME,
        object_name=parquet_object_path,
        data=io.BytesIO(data),
        length=len(data),
        content_type="application/octet-stream",
    )
    logger.info(
        f"Uploaded flattened USGS DataFrame to MinIO at {CURATED_BUCKET_NAME}/{parquet_object_path}"
    )
    return parquet_object_path


@task(task_id="create_postgis_table")
def create_postgis_table() -> None:
    """
    Creates the usgs_earthquakes table in PostGIS if it does not exist.
    Includes a geometry column for spatial data.
    """
    enable_postgis_sql, create_table_sql, create_index_sql = (
        create_postgis_table_helper()
    )

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(enable_postgis_sql)
            cur.execute(create_table_sql)
            cur.execute(create_index_sql)
    logger.info("Ensured usgs_earthquakes table and spatial index exist in PostGIS.")


@task(task_id="load_eq_to_postgres")
def load_eq_to_postgres(parquet_object_path: str) -> None:
    """
    Reads the curated Parquet from MinIO and upserts into Postgres.
    Uses ON CONFLICT (id) DO UPDATE so the task is safe to rerun.
    Requires a table created with the DDL in include/config/create_usgs_earthquakes.sql.
    """
    return None


with DAG(
    dag_id="usgs_to_minio_daily_http_operator",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="@daily",  # run at the top of every day, pulling yesterday's data at 00:00 UTC
    catchup=False,
    max_active_runs=1,
    tags=["usgs", "minio", "raw"],
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
        response_check=lambda response: response.status_code
        == 200,  # Raise AirflowException if not 200
        response_filter=lambda response: response.text,  # Pushed to XCom
        log_response=False,  # Avoid logging large JSON payloads in Airflow logs
    )

    validate_eq_payload_task = validate_eq_payload(fetch_usgs_events_task.output)
    upload_raw_eq_json_to_minio_task = upload_raw_eq_json_to_minio(
        validate_eq_payload_task
    )
    flatten_eq_json_to_df_task = flatten_eq_json_to_df(upload_raw_eq_json_to_minio_task)
    rename_df_columns_task = rename_df_columns(flatten_eq_json_to_df_task)
    upload_flattened_eq_to_minio_task = upload_flattened_eq_to_minio(
        rename_df_columns_task
    )
    create_postgis_table_task = create_postgis_table()
    load_eq_to_postgres_task = load_eq_to_postgres(upload_flattened_eq_to_minio_task)

    # DAG structure
    chain(
        fetch_usgs_events_task,
        validate_eq_payload_task,
        upload_raw_eq_json_to_minio_task,
        flatten_eq_json_to_df_task,
        rename_df_columns_task,
        upload_flattened_eq_to_minio_task,
        create_postgis_table_task,
        load_eq_to_postgres_task,
    )
