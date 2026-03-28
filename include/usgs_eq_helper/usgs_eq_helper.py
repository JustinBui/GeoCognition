"""
BUSINESS LOGIC HELPERS FOR USGS EARTHQUAKE PIPELINE
"""

import pandas as pd
import numpy as np
import json
from include.common import to_list
import logging

logger = logging.getLogger(__name__)


def validate_eq_payload_helper(raw_json_text: str) -> dict:
    """
    Helper for validate_eq_payload task
        - Validates JSON before returning it downstream
    """
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

    return payload


def flatten_eq_json_to_df_helper(payload: dict, eq_columns: list[str]) -> pd.DataFrame:
    """
    Helper for flatten_eq_json_to_df task
        - Flattens the USGS GeoJSON payload to a DataFrame with expected columns
    """
    features = payload.get("features", [])
    if not features:
        return pd.DataFrame(columns=eq_columns)

    df_features = pd.json_normalize(features)

    if "geometry.coordinates" in df_features.columns:
        coords = df_features["geometry.coordinates"].apply(
            lambda x: to_list(x) if isinstance(x, (list, tuple)) else x
        )
        df_features["longitude"] = coords.str[0]
        df_features["latitude"] = coords.str[1]
        df_features["depth_km"] = coords.str[2]
        df_features.drop(columns=["geometry.coordinates"], inplace=True)
    else:
        logger.warning(
            "No geometry.coordinates found; filling longitude, latitude, and depth_km with NaN"
        )
        df_features["longitude"] = np.nan
        df_features["latitude"] = np.nan
        df_features["depth_km"] = np.nan

    for col in eq_columns:
        if col not in df_features.columns:
            logger.warning(
                f"Column '{col}' missing from flattened DataFrame; filling with NaN"
            )
            df_features[col] = np.nan

    return df_features[eq_columns]


def create_postgis_table_helper() -> tuple[str, str, str]:
    """
    Helper for create_postgis_table task
        - Returns SQL statements to create the earthquakes table and spatial index
    """
    enable_postgis_sql = "CREATE EXTENSION IF NOT EXISTS postgis;"
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS usgs_earthquakes (
        id TEXT PRIMARY KEY,
        mag DOUBLE PRECISION,
        place TEXT,
        time BIGINT,
        updated BIGINT,
        tz TEXT,
        url TEXT,
        detail TEXT,
        felt DOUBLE PRECISION,
        cdi DOUBLE PRECISION,
        mmi DOUBLE PRECISION,
        alert TEXT,
        status TEXT,
        tsunami INTEGER,
        sig INTEGER,
        net TEXT,
        code TEXT,
        ids TEXT,
        sources TEXT,
        product_types TEXT,
        nst INTEGER,
        dmin DOUBLE PRECISION,
        rms DOUBLE PRECISION,
        gap INTEGER,
        magType TEXT,
        seismic_event TEXT,
        title TEXT,
        longitude DOUBLE PRECISION,
        latitude DOUBLE PRECISION,
        depth_km DOUBLE PRECISION,
        geom geometry(Point, 4326)
    );
    """
    create_index_sql = """
    CREATE INDEX IF NOT EXISTS idx_usgs_earthquakes_geom ON usgs_earthquakes USING GIST (geom);
    """
    return enable_postgis_sql, create_table_sql, create_index_sql
