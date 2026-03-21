"""Starter utilities for testing DAG Python callables with custom Airflow-like context.

Use these helpers to write your own test cases for functions that accept ``**context``.
"""

import json
from pathlib import Path

import pytest

from dags import usgs_eq_pipeline_dag as dag


class FakeTI:
    """Smallest useful TaskInstance mock for xcom_pull."""

    def __init__(self, xcom_values=None):
        self._xcom_values = xcom_values or {}

    def xcom_pull(self, task_ids=None):
        return self._xcom_values.get(task_ids)


def make_context(ds="2026-01-02", xcom_values=None, **extra):
    """Create fake Airflow context for PythonOperator callables."""
    context = {
        "ds": ds,
        "ti": FakeTI(xcom_values or {}),
    }
    context.update(extra)
    return context


@pytest.fixture
def fixtures_dir() -> Path:
    return Path(__file__).resolve().parent / "usgs_eq"


@pytest.fixture
def healthy_payload(fixtures_dir) -> str:
    return (fixtures_dir / "healthy_data.json").read_text(encoding="utf-8")


def test_example_validate_payload_context(healthy_payload):
    """Example: pass fake context directly into a DAG callable."""
    ctx = make_context(xcom_values={"fetch_usgs_events": healthy_payload})
    result = dag.validate_eq_payload(**ctx)
    assert json.loads(result).get("type") == "FeatureCollection"


def test_context_factory_allows_overrides():
    ctx = make_context(ds="2026-05-01", run_id="manual__demo")
    assert ctx["ds"] == "2026-05-01"
    assert ctx["run_id"] == "manual__demo"
    assert isinstance(ctx["ti"], FakeTI)