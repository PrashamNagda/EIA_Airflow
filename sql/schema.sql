-- schema.sql
-- Run this manually to set up the database, or let the DAG do it automatically.
-- Included here for documentation and version control purposes.

-- ── Database setup (run once as superuser) ────────────────────────────────────
-- CREATE DATABASE energy_db;
-- \c energy_db

-- ── Main fact table ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS electricity_generation (
    id             SERIAL PRIMARY KEY,
    month          VARCHAR(7)    NOT NULL,   -- 'YYYY-MM' e.g. '2024-03'
    fuel_type      VARCHAR(10)   NOT NULL,   -- EIA code: 'SUN', 'WND', 'NG', 'COL', 'NUC', 'HYC'
    fuel_label     VARCHAR(50),              -- Human-readable: 'Solar', 'Wind', etc.
    generation_mwh FLOAT,                   -- Megawatt-hours generated that month
    location       VARCHAR(10),             -- 'US' for national aggregate
    ingested_at    TIMESTAMP,               -- When this row was written by the pipeline
    UNIQUE (month, fuel_type)               -- Prevents duplicate rows on re-runs (idempotency)
);

-- Index on month for fast time-series queries
CREATE INDEX IF NOT EXISTS idx_gen_month     ON electricity_generation (month);
CREATE INDEX IF NOT EXISTS idx_gen_fuel_type ON electricity_generation (fuel_type);

-- ── Pipeline observability table ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id           SERIAL PRIMARY KEY,
    run_id       VARCHAR(100),   -- Airflow run_id
    rows_fetched INT,            -- Records returned by EIA API
    rows_loaded  INT,            -- New rows written to electricity_generation
    status       VARCHAR(20),   -- 'success' | 'failed'
    started_at   TIMESTAMP,
    finished_at  TIMESTAMP
);

-- ── Useful analytical queries ─────────────────────────────────────────────────

-- Total generation by source (all time)
-- SELECT fuel_label, ROUND(SUM(generation_mwh)::numeric / 1e6, 2) AS twh
-- FROM electricity_generation
-- GROUP BY fuel_label
-- ORDER BY twh DESC;

-- Year-over-year solar growth
-- SELECT
--     LEFT(month, 4)           AS year,
--     ROUND(SUM(generation_mwh)::numeric / 1e6, 2) AS solar_twh
-- FROM electricity_generation
-- WHERE fuel_type = 'SUN'
-- GROUP BY year
-- ORDER BY year;

-- Renewable share by month (solar + wind + hydro vs total)
-- SELECT
--     month,
--     ROUND(
--         100.0 * SUM(CASE WHEN fuel_type IN ('SUN','WND','HYC') THEN generation_mwh ELSE 0 END)
--              / NULLIF(SUM(generation_mwh), 0),
--         1
--     ) AS renewable_pct
-- FROM electricity_generation
-- GROUP BY month
-- ORDER BY month;
