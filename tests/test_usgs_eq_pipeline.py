# To run this file, be inside of your root directory. Then run 
#       pytest tests/test_usgs_eq_pipeline.py -q

import json
from pathlib import Path


import pandas as pd
import pytest
from include.constants import EQ_COLUMNS
from include.usgs_eg_helper import (
    flatten_eq_json_to_df_helper,
    validate_eq_payload_helper,
)


TEST_INPUTS_DIR = Path(__file__).parent / "usgs_eq_inputs"


def _read_input_text(file_name: str) -> str:
    return (TEST_INPUTS_DIR / file_name).read_text(encoding="utf-8")


def _read_input_json(file_name: str) -> dict:
    return json.loads(_read_input_text(file_name))


# ====================== Test cases for validate_eq_payload_helper ======================

@pytest.mark.parametrize(
    "file_name, expected_error",
    [
        ('empty_features.json', None),
        ("features_not_list_type.json", "'features' is not a list"),
        ("healthy_data.json", None),
        ("missing_features_key.json", "missing required keys"),
        ("missing_geometry_key.json", None),
        ("missing_metadata_key.json", "missing required keys"),
        ("missing_type_key.json", "missing required keys"),
        ("nothing.json", "USGS payload is empty"),
        ("some_missing_geometry_key.json", None),
        ("type_not_feature_collection.json", "type is not 'FeatureCollection'"),
    ],
)
def test_validate_eq_payload_helper(file_name: str, expected_error: str | None) -> None:
    raw_json_text = _read_input_text(file_name)

    if expected_error is not None:
        with pytest.raises(ValueError, match=expected_error):
            validate_eq_payload_helper(raw_json_text)
        return

    payload = validate_eq_payload_helper(raw_json_text)
    assert payload["type"] == "FeatureCollection"
    assert isinstance(payload["features"], list)

# ====================== Test cases for flatten_eq_json_to_df_helper ======================

@pytest.mark.parametrize(
    "file_name, expected_len",
    [
        ("empty_features.json", 0),
        ("features_not_list_type.json", 0),
        ("healthy_data.json", 152),
        ("missing_features_key.json", 0),
        ("missing_geometry_key.json", 45),
        ("missing_metadata_key.json", 45),
        ("missing_type_key.json", 70),
        ("some_missing_geometry_key.json", 152),
        ("type_not_feature_collection.json", 45),
    ],
)
def test_flatten_eq_json_to_df_helper(file_name: str, expected_len: int) -> None:
    payload = _read_input_json(file_name)

    df = flatten_eq_json_to_df_helper(payload, EQ_COLUMNS)

    assert isinstance(df, pd.DataFrame)
    assert len(df) == expected_len
    assert list(df.columns) == EQ_COLUMNS

# ====================== INTEGRATION TESTING ======================

@pytest.mark.parametrize(
    "file_name, expected_validation_error, expected_len",
    [
        ("healthy_data.json", None, 152),
        ("empty_features.json", None, 0),
        ("missing_geometry_key.json", None, 45),
        ("some_missing_geometry_key.json", None, 152),
        ("missing_type_key.json", "missing required keys", None),
        ("missing_metadata_key.json", "missing required keys", None),
        ("missing_features_key.json", "missing required keys", None),
        ("features_not_list_type.json", "'features' is not a list", None),
        ("type_not_feature_collection.json", "type is not 'FeatureCollection'", None),
        ("nothing.json", "USGS payload is empty", None),
    ],
)
def test_helper_integration_like_dag(
    file_name: str, expected_validation_error: str | None, expected_len: int | None
) -> None:
    raw_json_text = _read_input_text(file_name)

    if expected_validation_error is not None:
        with pytest.raises(ValueError, match=expected_validation_error):
            validate_eq_payload_helper(raw_json_text)
        return

    payload = validate_eq_payload_helper(raw_json_text)
    df = flatten_eq_json_to_df_helper(payload, EQ_COLUMNS)

    assert isinstance(df, pd.DataFrame)
    assert len(df) == expected_len
    assert list(df.columns) == EQ_COLUMNS
