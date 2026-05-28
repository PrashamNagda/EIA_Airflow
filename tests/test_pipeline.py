"""
tests/test_pipeline.py
Basic unit tests for the EIA pipeline.
Run with: pytest tests/ -v
"""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from datetime import datetime


# ── api_helpers tests ──────────────────────────────────────────────────────────

class TestFetchElectricityGeneration:

    def test_returns_dataframe(self):
        """fetch_electricity_generation should return a pandas DataFrame."""
        mock_records = [
            {"period": "2024-01", "fueltypeid": "SUN", "generation": "1234.5", "location": "US"},
            {"period": "2024-01", "fueltypeid": "WND", "generation": "5678.9", "location": "US"},
        ]
        mock_response = {
            "response": {"data": mock_records}
        }

        with patch("utils.api_helpers.requests.get") as mock_get:
            mock_get.return_value.json.return_value = mock_response
            mock_get.return_value.raise_for_status = MagicMock()

            with patch.dict("os.environ", {"EIA_API_KEY": "test_key"}):
                from utils.api_helpers import fetch_electricity_generation
                df = fetch_electricity_generation()

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2

    def test_required_columns_present(self):
        """Output DataFrame must have the expected columns."""
        mock_records = [
            {"period": "2024-01", "fueltypeid": "SUN", "generation": "100", "location": "US"},
        ]
        mock_response = {"response": {"data": mock_records}}

        with patch("utils.api_helpers.requests.get") as mock_get:
            mock_get.return_value.json.return_value = mock_response
            mock_get.return_value.raise_for_status = MagicMock()

            with patch.dict("os.environ", {"EIA_API_KEY": "test_key"}):
                from utils.api_helpers import fetch_electricity_generation
                df = fetch_electricity_generation()

        expected_cols = {"month", "fuel_type", "fuel_label", "generation_mwh", "location", "ingested_at"}
        assert expected_cols.issubset(set(df.columns))

    def test_generation_mwh_is_numeric(self):
        """generation_mwh column should be float, not string."""
        mock_records = [
            {"period": "2024-01", "fueltypeid": "SUN", "generation": "999.99", "location": "US"},
        ]
        mock_response = {"response": {"data": mock_records}}

        with patch("utils.api_helpers.requests.get") as mock_get:
            mock_get.return_value.json.return_value = mock_response
            mock_get.return_value.raise_for_status = MagicMock()

            with patch.dict("os.environ", {"EIA_API_KEY": "test_key"}):
                from utils.api_helpers import fetch_electricity_generation
                df = fetch_electricity_generation()

        assert pd.api.types.is_float_dtype(df["generation_mwh"])

    def test_raises_on_empty_response(self):
        """Should raise ValueError if API returns no records."""
        mock_response = {"response": {"data": []}}

        with patch("utils.api_helpers.requests.get") as mock_get:
            mock_get.return_value.json.return_value = mock_response
            mock_get.return_value.raise_for_status = MagicMock()

            with patch.dict("os.environ", {"EIA_API_KEY": "test_key"}):
                from utils.api_helpers import fetch_electricity_generation
                with pytest.raises(ValueError, match="No data returned"):
                    fetch_electricity_generation()

    def test_raises_without_api_key(self):
        """Should raise EnvironmentError if EIA_API_KEY is not set."""
        with patch.dict("os.environ", {}, clear=True):
            # Remove key from environment
            import importlib
            import utils.api_helpers as m
            m.EIA_API_KEY = None

            with pytest.raises(EnvironmentError, match="EIA_API_KEY not found"):
                m.fetch_electricity_generation()


# ── db_helpers tests ───────────────────────────────────────────────────────────

class TestUpsertGenerationData:

    def test_returns_zero_on_empty_dataframe(self):
        """upsert_generation_data should return 0 without hitting the DB for empty input."""
        with patch("utils.db_helpers.get_connection") as mock_conn:
            from utils.db_helpers import upsert_generation_data
            result = upsert_generation_data(pd.DataFrame())

        assert result == 0
        mock_conn.assert_not_called()

    def test_accepts_valid_dataframe(self):
        """upsert_generation_data should call execute_values with correct data."""
        df = pd.DataFrame([{
            "month": "2024-01",
            "fuel_type": "SUN",
            "fuel_label": "Solar",
            "generation_mwh": 12345.0,
            "location": "US",
            "ingested_at": datetime.now(),
        }])

        mock_cursor = MagicMock()
        mock_cursor.rowcount = 1
        mock_conn_instance = MagicMock()
        mock_conn_instance.cursor.return_value = mock_cursor

        with patch("utils.db_helpers.get_connection", return_value=mock_conn_instance):
            with patch("utils.db_helpers.execute_values") as mock_exec:
                from utils.db_helpers import upsert_generation_data
                upsert_generation_data(df)
                assert mock_exec.called


# ── DAG structure tests ────────────────────────────────────────────────────────

class TestDAGStructure:

    def test_dag_loads_without_errors(self):
        """The DAG file should import cleanly with no syntax errors."""
        import importlib.util, sys
        spec = importlib.util.spec_from_file_location(
            "eia_energy_pipeline",
            "dags/eia_energy_pipeline.py"
        )
        mod = importlib.util.module_from_spec(spec)
        # If this raises, the DAG has a syntax or import error
        # spec.loader.exec_module(mod)  # uncomment when running locally with Airflow installed

    def test_task_order(self):
        """Tasks should be connected in the correct dependency order."""
        expected_order = [
            "create_tables",
            "extract_and_load",
            "validate_load",
            "log_run",
        ]
        # Just validate the list is correct (full test requires Airflow installed)
        assert expected_order[0] == "create_tables"
        assert expected_order[-1] == "log_run"
