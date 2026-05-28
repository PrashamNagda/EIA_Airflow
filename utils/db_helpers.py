"""
db_helpers.py
Handles all PostgreSQL interactions: creating tables, upserting data, querying.
"""

import os
import logging
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


def get_connection() -> psycopg2.extensions.connection:
    """
    Returns a live psycopg2 connection using credentials from .env.
    Always close the connection after use (or use a context manager).
    """
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME", "energy_db"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD", ""),
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
    )


def create_tables() -> None:
    """
    Creates all required tables if they don't already exist.
    Safe to re-run — uses CREATE TABLE IF NOT EXISTS.
    """
    conn = get_connection()
    cur = conn.cursor()

    # Main fact table: one row per (month, fuel_type) pair
    cur.execute("""
        CREATE TABLE IF NOT EXISTS electricity_generation (
            id             SERIAL PRIMARY KEY,
            month          VARCHAR(7)    NOT NULL,   -- 'YYYY-MM'
            fuel_type      VARCHAR(10)   NOT NULL,   -- EIA code e.g. 'SUN'
            fuel_label     VARCHAR(50),              -- Human-readable e.g. 'Solar'
            generation_mwh FLOAT,                   -- MWh generated that month
            location       VARCHAR(10),
            ingested_at    TIMESTAMP,
            UNIQUE (month, fuel_type)               -- prevents duplicate rows on re-runs
        );
    """)

    # Pipeline run log — useful for debugging and showing on your resume
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id           SERIAL PRIMARY KEY,
            run_id       VARCHAR(100),
            rows_fetched INT,
            rows_loaded  INT,
            status       VARCHAR(20),   -- 'success' or 'failed'
            started_at   TIMESTAMP,
            finished_at  TIMESTAMP
        );
    """)

    conn.commit()
    cur.close()
    conn.close()
    logger.info("Tables verified/created.")


def upsert_generation_data(df: pd.DataFrame) -> int:
    """
    Bulk-upserts a DataFrame into electricity_generation.
    Uses INSERT ... ON CONFLICT DO NOTHING so re-running is always safe
    (idempotent — a key concept in data engineering).

    Returns:
        Number of newly inserted rows.
    """
    if df.empty:
        logger.warning("upsert_generation_data called with empty DataFrame.")
        return 0

    conn = get_connection()
    cur = conn.cursor()

    rows = [
        (
            row["month"],
            row["fuel_type"],
            row["fuel_label"],
            row["generation_mwh"],
            row["location"],
            row["ingested_at"],
        )
        for _, row in df.iterrows()
    ]

    # execute_values is much faster than looping cur.execute for large datasets
    execute_values(
        cur,
        """
        INSERT INTO electricity_generation
            (month, fuel_type, fuel_label, generation_mwh, location, ingested_at)
        VALUES %s
        ON CONFLICT (month, fuel_type) DO NOTHING
        """,
        rows,
    )

    rows_inserted = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()

    logger.info(f"Upserted {len(rows)} rows → {rows_inserted} new rows written.")
    return rows_inserted


def log_pipeline_run(
    run_id: str,
    rows_fetched: int,
    rows_loaded: int,
    status: str,
    started_at,
    finished_at,
) -> None:
    """Records metadata about each pipeline run for observability."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO pipeline_runs
            (run_id, rows_fetched, rows_loaded, status, started_at, finished_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (run_id, rows_fetched, rows_loaded, status, started_at, finished_at),
    )
    conn.commit()
    cur.close()
    conn.close()


def get_row_count() -> int:
    """Returns current row count of the electricity_generation table."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM electricity_generation;")
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    return count


def query_to_dataframe(sql: str) -> pd.DataFrame:
    """Runs a SQL query and returns results as a pandas DataFrame."""
    conn = get_connection()
    df = pd.read_sql(sql, conn)
    conn.close()
    return df
