import io
import json
import pendulum
import pandas as pd
import numpy as np


def build_partition_path(ds: str, filename: str) -> str:
    dt = pendulum.parse(ds, tz="UTC")
    return f"year={dt:%Y}/month={dt:%m}/day={dt:%d}/{filename}"


def parse_eq_payload(raw_json_text: str) -> dict:
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

    return payload


def to_list(v):
    if isinstance(v, str):
        try:
            return json.loads(v)
        except Exception:
            return np.nan
    return v


def flatten_payload_to_df(payload: dict, eq_columns: list[str]) -> pd.DataFrame:
    features = payload.get("features", [])
    if not features:
        return pd.DataFrame(columns=eq_columns)

    df_features = pd.json_normalize(features)

    if "geometry.coordinates" in df_features.columns:
        coords = df_features["geometry.coordinates"].apply(
            lambda x: to_list(x) if pd.notna(x) else x
        )
        df_features["longitude"] = coords.str[0]
        df_features["latitude"] = coords.str[1]
        df_features["depth_km"] = coords.str[2]
        df_features.drop(columns=["geometry.coordinates"], inplace=True)
    else:
        df_features["longitude"] = np.nan
        df_features["latitude"] = np.nan
        df_features["depth_km"] = np.nan

    for col in eq_columns:
        if col not in df_features.columns:
            df_features[col] = np.nan

    return df_features[eq_columns]


def dataframe_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    return buf.getvalue()