"""
api_helpers.py
Handles all communication with the EIA (Energy Information Administration) API.
Docs: https://www.eia.gov/opendata/documentation.php
"""

import os
import logging
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

EIA_API_KEY = os.getenv("EIA_API_KEY")

# Human-readable labels for fuel type codes returned by the API
FUEL_TYPE_LABELS = {
    "SUN": "Solar",
    "WND": "Wind",
    "NG":  "Natural Gas",
    "COL": "Coal",
    "NUC": "Nuclear",
    "HYC": "Hydropower",
}

BASE_URL = "https://api.eia.gov/v2/electricity/electric-power-operational-data/data/"


def fetch_electricity_generation(
    start_period: str = "2019-01",
    end_period: str = "2024-12",
    fuel_types: list = None,
) -> pd.DataFrame:
    """
    Fetches monthly US electricity generation by energy source from EIA API.

    Args:
        start_period: First month to fetch, format 'YYYY-MM'.
        end_period:   Last month to fetch, format 'YYYY-MM'.
        fuel_types:   List of EIA fuel type codes. Defaults to major 6 sources.

    Returns:
        Cleaned pandas DataFrame with columns:
            month, fuel_type, fuel_label, generation_mwh, location, ingested_at
    """
    if not EIA_API_KEY:
        raise EnvironmentError(
            "EIA_API_KEY not found. Add it to your .env file. "
            "Get a free key at https://www.eia.gov/opendata/"
        )

    if fuel_types is None:
        fuel_types = list(FUEL_TYPE_LABELS.keys())

    params = {
        "api_key": EIA_API_KEY,
        "frequency": "monthly",
        "data[0]": "generation",
        "facets[location][]": "US",
        "facets[sectorid][]": "99",   # All sectors combined
        "start": start_period,
        "end": end_period,
        "sort[0][column]": "period",
        "sort[0][direction]": "asc",
        "length": 5000,
    }

    # Add each fuel type as a separate query param
    for i, fuel in enumerate(fuel_types):
        params[f"facets[fueltypeid][{i}]"] = fuel

    logger.info(f"Fetching EIA data: {start_period} → {end_period}, sources: {fuel_types}")

    response = requests.get(BASE_URL, params=params, timeout=30)
    response.raise_for_status()

    payload = response.json()
    records = payload.get("response", {}).get("data", [])

    if not records:
        raise ValueError(
            f"EIA API returned no records. Check your API key and date range. "
            f"Raw response: {payload}"
        )

    logger.info(f"Received {len(records)} records from EIA API.")

    df = pd.DataFrame(records)

    # Rename to clean column names
    df = df.rename(columns={
        "period":      "month",
        "fueltypeid":  "fuel_type",
        "generation":  "generation_mwh",
        "location":    "location",
    })

    # Keep only what we need
    df = df[["month", "fuel_type", "generation_mwh", "location"]]

    # Add human-readable label
    df["fuel_label"] = df["fuel_type"].map(FUEL_TYPE_LABELS).fillna(df["fuel_type"])

    # Cast generation to float; mark invalid values as NaN
    df["generation_mwh"] = pd.to_numeric(df["generation_mwh"], errors="coerce")

    # Timestamp for lineage tracking
    df["ingested_at"] = pd.Timestamp.now()

    logger.info(f"Returning cleaned DataFrame with {len(df)} rows.")
    return df
