import os
import pandas as pd
from box.exceptions import BoxValueError
import io
import yaml
from box import ConfigBox
from pathlib import Path
from include.constants import CONFIG_FILE_PATH
from minio import Minio
import logging
import json
import numpy as np
from typing import Union

logger = logging.getLogger(__name__)


def read_yaml(path_to_yaml: Path, verbose=True) -> ConfigBox:
    """
    Reads yaml file and returns a ConfigBox object.
    """
    try:
        with open(path_to_yaml) as yaml_file:
            content = yaml.safe_load(yaml_file)
            if verbose:
                logging.info(f"yaml file: {path_to_yaml} loaded successfully")
            return ConfigBox(content)
    except BoxValueError:
        raise ValueError("yaml file is empty")
    except Exception as e:
        raise e


def create_directories(path_to_directories: list, verbose=True) -> None:
    """
    Create list of directories
    """
    for path in path_to_directories:
        os.makedirs(path, exist_ok=True)
        if verbose:
            logging.info(f"created directory at: {path}")


def to_list(v):
    """
    Helper to convert a JSON string representation of a list back to a Python list, if needed.
    """
    if isinstance(v, str):
        try:
            return json.loads(v)
        except Exception:
            return np.nan
    return v


def dataframe_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    """
    Helper to convert a DataFrame to Parquet bytes for in-memory upload to MinIO
    """
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    return buf.getvalue()


# --------------------- MINIO HELPER FUNCIONS --------------------


def get_minio_client() -> Minio:
    """
    Helper to create and return a MinIO client using configuration from the YAML file and environment variables.
    """
    endpoint = read_yaml(CONFIG_FILE_PATH).data_ingestion.minio_endpoint
    access_key = os.getenv("MINIO_ACCESS_KEY", "minio")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minio123")

    return Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False,
    )


def ensure_bucket_exists(client: Minio, bucket_name: str) -> None:
    """
    Create bucket if it does not already exist.
    """
    found = client.bucket_exists(bucket_name)
    if not found:
        client.make_bucket(bucket_name)


def get_object_path(context: dict, postfix: str, filetype: str) -> str:
    """
    Helper for building partitioned object paths in MinIO based on the execution date (ds) and filename.
     - postfix can be a one more description of the file (e.g. "raw_usgs_eq_data") to make it more identifiable in MinIO
     - filetype can be ".json" or ".parquet" depending on the data format being uploaded into MinIO
    """

    start_dt = context["data_interval_start"].start_of("day")
    start_ts = start_dt.strftime(
        "%H:%M:%S"
    )  # Midnight of the day (e.g. 2025-01-01T00:00:00)
    end_ts = start_dt.end_of("day").strftime(
        "%H:%M:%S"
    )  # End of the day (e.g. 2025-01-01T23:59:59)

    return f"year={start_dt:%Y}/month={start_dt:%m}/day={start_dt:%d}/{start_ts}_to_{end_ts}_{postfix}.{filetype}"


def minio_object_exists(
    client: Minio,
    bucket_name: str,
    object_path: str,
) -> bool:
    """
    Validates and uploads a data object (dict or DataFrame) to MinIO at the specified bucket and object path.
    """
    # Creating bucket
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    # Check if object already exists
    found = False
    try:
        client.stat_object(bucket_name, object_path)
        found = True
    except Exception:
        found = False

    return found


def upload_file_to_minio(
    client: Minio,
    bucket_name: str,
    object_path: str,
    data_object: Union[dict, pd.DataFrame],
    data_content_type: str,
) -> None:
    """
    Uploads a data object (dict or DataFrame) to MinIO at the specified bucket and object path.
    """
    client = get_minio_client()
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    client.put_object(
        bucket_name=bucket_name,
        object_name=object_path,
        data=io.BytesIO(data_object),
        length=len(data_object),
        content_type=data_content_type,
    )
