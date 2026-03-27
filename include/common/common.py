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
import pendulum
import json
import numpy as np


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


def debug_context(**context) -> None:
    logging.info("Context keys: %s", sorted(context.keys()))
    logging.info("ds=%s", context.get("ds"))
    logging.info("data_interval_start=%s", context.get("data_interval_start"))
    logging.info("data_interval_end=%s", context.get("data_interval_end"))


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


def get_partition_path(ds: str, filename: str) -> str:
    """
    Helper for building partitioned object paths in MinIO based on the execution date (ds) and filename.
    """
    dt = pendulum.parse(ds, tz="UTC")
    return f"year={dt:%Y}/month={dt:%m}/day={dt:%d}/{filename}"


# def upload_file_to_minio(
#     local_file_path: str,
#     bucket_name: str,
#     object_name: str,
# ) -> None:
#     """
#     Upload a local file to MinIO.
#     """
#     client = get_minio_client()
#     ensure_bucket_exists(client, bucket_name)

#     client.fput_object(
#         bucket_name=bucket_name,
#         object_name=object_name,
#         file_path=local_file_path,
#     )

#     print(f"Uploaded {local_file_path} to s3://{bucket_name}/{object_name}")
