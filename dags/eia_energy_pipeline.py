"""
eia_energy_pipeline.py
─────────────────────────────────────────────────────────────────────────────
Airflow DAG: US Energy Generation ELT Pipeline
─────────────────────────────────────────────────────────────────────────────
Schedule : Weekly (every Sunday at midnight)
Source   : EIA (Energy Information Administration) Open Data API
Sink     : PostgreSQL → electricity_generation table

Pipeline steps:
  1. create_tables    → ensure DB schema exists
  2. extract_and_load → pull data from EIA API, upsert into PostgreSQL
  3. validate_load    → sanity-check row count; fail loudly if something's wrong
  4. log_run          → record run metadata for observability

Author: <your name>
"""

import sys
import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ── Make utils importable from Airflow's DAG folder ──────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from utils.api_helpers import fetch_electricity_generation
from utils.db_helpers import (
    create_tables,
    upsert_generation_data,
    log_pipeline_run,
    get_row_count,
)

logger = logging.getLogger(__name__)

# ── Pipeline config ───────────────────────────────────────────────────────────

FETCH_START = "2019-01"
FETCH_END   = "2024-12"

# ── Default args (applied to every task unless overridden) ────────────────────

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,   # 3min → 6min → 12min between retries
    "email_on_failure": False,
    "email_on_retry": False,
}

# ── Task callables ─────────────────────────────────────────────────────────────

def task_create_tables(**context):
    """
    Task 1 — Schema Setup
    Creates the electricity_generation and pipeline_runs tables if they
    don't exist. Uses CREATE TABLE IF NOT EXISTS so this is always safe to run.
    """
    logger.info("Ensuring database schema is up to date...")
    create_tables()
    logger.info("Schema ready.")


def task_extract_and_load(**context):
    """
    Task 2 — Extract & Load
    Pulls electricity generation data from the EIA API and upserts
    it into PostgreSQL. Stores row counts in XCom for downstream tasks.

    XCom pushes:
        rows_fetched (int) — how many rows came back from the API
        rows_loaded  (int) — how many new rows were written to Postgres
    """
    started_at = datetime.now()
    logger.info(f"Fetching EIA data: {FETCH_START} → {FETCH_END}")

    df = fetch_electricity_generation(
        start_period=FETCH_START,
        end_period=FETCH_END,
    )
    rows_fetched = len(df)
    logger.info(f"Fetched {rows_fetched} records from EIA API.")

    rows_loaded = upsert_generation_data(df)
    logger.info(f"Loaded {rows_loaded} new rows into PostgreSQL.")

    # Push to XCom so downstream tasks can read these values
    context["ti"].xcom_push(key="rows_fetched", value=rows_fetched)
    context["ti"].xcom_push(key="rows_loaded",  value=rows_loaded)
    context["ti"].xcom_push(key="started_at",   value=str(started_at))


def task_validate_load(**context):
    """
    Task 3 — Validation
    Confirms the table is non-empty after the load. Raises an exception
    (which Airflow treats as a task failure) if something went wrong.
    This pattern is called a data quality check.
    """
    total_rows = get_row_count()
    logger.info(f"Validation: electricity_generation has {total_rows} total rows.")

    if total_rows == 0:
        raise ValueError(
            "VALIDATION FAILED: electricity_generation table is empty after load. "
            "Check extract_and_load logs for errors."
        )

    rows_loaded = context["ti"].xcom_pull(
        task_ids="extract_and_load", key="rows_loaded"
    )
    logger.info(
        f"Validation passed. Total rows in table: {total_rows:,} | "
        f"New rows this run: {rows_loaded}"
    )


def task_log_run(**context):
    """
    Task 4 — Observability
    Writes a summary row to pipeline_runs so you can query run history.
    This is what data teams use to monitor pipeline health over time.
    """
    ti = context["ti"]
    rows_fetched = ti.xcom_pull(task_ids="extract_and_load", key="rows_fetched") or 0
    rows_loaded  = ti.xcom_pull(task_ids="extract_and_load", key="rows_loaded")  or 0
    started_at   = ti.xcom_pull(task_ids="extract_and_load", key="started_at")

    log_pipeline_run(
        run_id=str(context["run_id"]),
        rows_fetched=rows_fetched,
        rows_loaded=rows_loaded,
        status="success",
        started_at=started_at,
        finished_at=str(datetime.now()),
    )
    logger.info("Pipeline run logged to pipeline_runs table.")


# ── DAG definition ─────────────────────────────────────────────────────────────

with DAG(
    dag_id="eia_energy_pipeline",
    description="Weekly ELT: EIA API → PostgreSQL | US electricity generation by source",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@weekly",
    catchup=False,                          # Don't backfill all missed weekly runs
    max_active_runs=1,                      # Prevent overlapping runs
    tags=["energy", "eia", "ELT", "tesla"],
    doc_md=__doc__,                         # Shows this docstring in the Airflow UI
) as dag:

    create_tables_task = PythonOperator(
        task_id="create_tables",
        python_callable=task_create_tables,
    )

    extract_and_load_task = PythonOperator(
        task_id="extract_and_load",
        python_callable=task_extract_and_load,
    )

    validate_task = PythonOperator(
        task_id="validate_load",
        python_callable=task_validate_load,
    )

    log_run_task = PythonOperator(
        task_id="log_run",
        python_callable=task_log_run,
        trigger_rule="all_success",          # Only log if all prior tasks passed
    )

    # ── Task dependency chain ──────────────────────────────────────────────────
    #
    #   create_tables → extract_and_load → validate_load → log_run
    #
    create_tables_task >> extract_and_load_task >> validate_task >> log_run_task
