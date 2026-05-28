# ⚡ US Energy Generation ELT Pipeline

![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![Airflow](https://img.shields.io/badge/Apache_Airflow-2.8-017CEE?logo=apacheairflow&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?logo=postgresql&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-1.31-FF4B4B?logo=streamlit&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green)

An automated **ELT (Extract → Load → Transform)** pipeline that ingests real US electricity generation data from the [EIA Open Data API](https://www.eia.gov/opendata/), loads it into PostgreSQL, and visualizes trends in an interactive Streamlit dashboard — all orchestrated by **Apache Airflow**.

---

## 📊 Dashboard Preview

> *Solar vs. wind vs. fossil fuel generation trends, renewable share over time, and annual breakdowns — all updating automatically every week.*

<!-- Add a screenshot here once you run it:
![Dashboard Screenshot](docs/dashboard-screenshot.png)
-->

---

## 🏗️ Architecture

```
┌─────────────────┐     ┌──────────────────────────────────────────────────┐
│                 │     │              Apache Airflow DAG                   │
│   EIA Open      │────▶│                                                   │
│   Data API      │     │  create_tables ──▶ extract_and_load ──▶ validate  │
│   (free)        │     │                         │                   │     │
│                 │     │                         ▼                   ▼     │
└─────────────────┘     │                     log_run (on success)         │
                        └──────────────────────────────────────────────────┘
                                                  │
                                                  ▼
                                        ┌──────────────────┐
                                        │   PostgreSQL     │
                                        │  energy_db       │
                                        │  ─────────────── │
                                        │  electricity_    │
                                        │  generation      │
                                        │  pipeline_runs   │
                                        └──────────────────┘
                                                  │
                                                  ▼
                                        ┌──────────────────┐
                                        │   Streamlit      │
                                        │   Dashboard      │
                                        └──────────────────┘
```

### Pipeline Steps

| Step | Task ID | What it does |
|------|---------|-------------|
| 1 | `create_tables` | Creates PostgreSQL schema if not already present |
| 2 | `extract_and_load` | Calls EIA API, cleans data with pandas, bulk-upserts into PostgreSQL |
| 3 | `validate_load` | Confirms row count > 0; fails loudly if something went wrong |
| 4 | `log_run` | Writes run metadata (rows fetched/loaded, timestamps) to `pipeline_runs` |

---

## ✨ Key Features

- **Idempotent loads** — uses `INSERT ... ON CONFLICT DO NOTHING` so re-running the pipeline never creates duplicate rows
- **Retry logic** — tasks automatically retry up to 2× with exponential backoff on failure
- **Data validation** — post-load quality check catches silent failures before they reach the dashboard
- **Observability** — every pipeline run is logged with row counts and timestamps
- **5 energy sources** — Solar, Wind, Natural Gas, Coal, Nuclear (2019–2024)
- **Interactive dashboard** — filter by source and year; KPI metrics, time-series, pie chart, and YoY bar chart

---

## 🗂️ Project Structure

```
eia-airflow-pipeline/
├── dags/
│   └── eia_energy_pipeline.py   # Airflow DAG — 4 tasks, weekly schedule
├── utils/
│   ├── api_helpers.py           # EIA API client + data cleaning
│   └── db_helpers.py            # PostgreSQL connection, upsert, queries
├── dashboard/
│   └── app.py                   # Streamlit dashboard (5 charts)
├── sql/
│   └── schema.sql               # Table definitions + useful analytical queries
├── tests/
│   └── test_pipeline.py         # Unit tests (pytest)
├── .env.example                 # Environment variable template
├── requirements.txt
└── README.md
```

---

## 🚀 Getting Started

### Prerequisites

- macOS with Python 3.11
- [Homebrew](https://brew.sh/)
- PostgreSQL 15 (`brew install postgresql@15`)
- Free [EIA API key](https://www.eia.gov/opendata/)

### 1. Clone & set up environment

```bash
git clone https://github.com/YOUR_USERNAME/eia-airflow-pipeline.git
cd eia-airflow-pipeline

python3 -m venv venv
source venv/bin/activate

pip install -r requirements.txt
```

### 2. Configure environment variables

```bash
cp .env.example .env
# Edit .env with your EIA API key and PostgreSQL credentials
```

### 3. Create the PostgreSQL database

```bash
createdb energy_db
```

### 4. Install & start Airflow

```bash
export AIRFLOW_HOME=~/airflow
airflow db init

airflow users create \
  --username admin --firstname Admin --lastname User \
  --role Admin --email admin@example.com --password admin

# In two separate terminal tabs:
airflow scheduler
airflow webserver --port 8080
```

### 5. Add your DAG

```bash
cp dags/eia_energy_pipeline.py ~/airflow/dags/
cp -r utils ~/airflow/
```

Open [http://localhost:8080](http://localhost:8080), find `eia_energy_pipeline`, and trigger a manual run.

### 6. Run the dashboard

```bash
streamlit run dashboard/app.py
```

Open [http://localhost:8501](http://localhost:8501).

---

## 🧪 Running Tests

```bash
pytest tests/ -v
```

---

## 📈 Sample Analytical Queries

```sql
-- Year-over-year solar growth
SELECT
    LEFT(month, 4) AS year,
    ROUND(SUM(generation_mwh) / 1e6, 2) AS solar_twh
FROM electricity_generation
WHERE fuel_type = 'SUN'
GROUP BY year ORDER BY year;

-- Monthly renewable share
SELECT
    month,
    ROUND(
        100.0 * SUM(CASE WHEN fuel_type IN ('SUN','WND','HYC') THEN generation_mwh ELSE 0 END)
             / NULLIF(SUM(generation_mwh), 0), 1
    ) AS renewable_pct
FROM electricity_generation
GROUP BY month ORDER BY month;
```

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|-----------|
| Orchestration | Apache Airflow 2.8 |
| Language | Python 3.11 |
| Data processing | pandas |
| Database | PostgreSQL 15 |
| DB driver | psycopg2 |
| Dashboard | Streamlit + Plotly |
| Testing | pytest |
| Data source | EIA Open Data API (free) |

---

## 📄 License

MIT — free to use for any purpose.

---

*Built as a portfolio project demonstrating ELT pipeline design, workflow orchestration, and data visualization skills.*
