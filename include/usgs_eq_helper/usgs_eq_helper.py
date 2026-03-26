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
        logger.warning("No geometry.coordinates found; filling longitude, latitude, and depth_km with NaN")
        df_features["longitude"] = np.nan
        df_features["latitude"] = np.nan
        df_features["depth_km"] = np.nan

    for col in eq_columns:
        if col not in df_features.columns:
            logger.warning(f"Column '{col}' missing from flattened DataFrame; filling with NaN")
            df_features[col] = np.nan

    return df_features[eq_columns]