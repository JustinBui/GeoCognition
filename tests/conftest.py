import pytest
from unittest.mock import MagicMock


@pytest.fixture
def mock_minio_client(monkeypatch):
    mock_client = MagicMock()

    # Default behaviors
    mock_client.bucket_exists.return_value = True
    mock_client.get_object.return_value.read.return_value = (
        b'{"type":"FeatureCollection","features":[]}'
    )
    mock_client.get_object.return_value.close.return_value = None
    mock_client.get_object.return_value.release_conn.return_value = None

    monkeypatch.setattr(
        "include.common.get_minio_client",
        lambda: mock_client,
    )

    return mock_client


@pytest.fixture
def mock_airflow_context(monkeypatch):
    monkeypatch.setattr(
        "airflow.sdk.get_current_context",
        lambda: {"ds": "2026-01-01"},
    )


@pytest.fixture
def mock_postgres(monkeypatch):
    mock_hook = MagicMock()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()

    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_hook.get_conn.return_value.__enter__.return_value = mock_conn

    monkeypatch.setattr(
        "airflow.providers.postgres.hooks.postgres.PostgresHook",
        lambda *args, **kwargs: mock_hook,
    )

    return mock_hook
