# Flight Ops — Near Real-Time Flight Data Pipeline (Airflow + Snowflake)

A near real-time ETL pipeline, orchestrated end-to-end with **Apache Airflow**, that pulls live flight state data from the public **OpenSky Network API**, moves it through a **Bronze → Silver → Gold** medallion architecture, and loads the aggregated result into **Snowflake** for dashboarding.

The entire stack runs locally via **Docker Compose** (Airflow webserver, scheduler, and a Postgres metadata database), with each pipeline stage implemented as an independent, testable Python task.

---

## Architecture

```
OpenSky API
     │
     ▼
┌─────────────┐   raw JSON    ┌─────────────┐   clean CSV   ┌─────────────┐   aggregated CSV   ┌─────────────┐
│   Bronze    │ ────────────▶ │   Silver    │ ────────────▶ │    Gold     │ ──────────────────▶│  Snowflake  │
│ (raw dump)  │               │ (cleaned)   │               │(aggregated) │                     │ FLIGHT_KPIS │
└─────────────┘               └─────────────┘               └─────────────┘                     └─────────────┘
```

| Layer    | Format | Purpose                                                             |
|----------|--------|----------------------------------------------------------------------|
| Bronze   | JSON   | Raw, unmodified API response — full fidelity snapshot of each pull   |
| Silver   | CSV    | Cleaned, column-selected, typed data — one row per aircraft state    |
| Gold     | CSV    | Business-ready data — aggregated per country, per time window        |
| Snowflake| Table  | Durable, query-ready KPI store, upserted via `MERGE`                 |

The DAG (`flights_ops_medallion_pipe`) runs on a **30-minute schedule** (`*/30 * * * *`) and executes four tasks strictly in sequence:

```
bronze_ingest  →  silver_transform  →  gold_aggregate  →  load_gold_to_snowflake
```

Each task is a plain Python function (no custom operators), orchestrated with Airflow's `PythonOperator`. Data is **not** passed through global variables or hardcoded paths between tasks — each stage pushes the file path it produced onto **XCom**, and the next stage pulls it by key. This keeps every task independently testable and avoids tight coupling to the DAG's own structure.

---

## Pipeline stages in detail

### 1. Bronze — Ingestion (`scripts/bronze_ingest.py`)

- Calls `GET https://opensky-network.org/api/states/all` (30s timeout) to pull a live snapshot of global flight states — no mock or synthetic data.
- Writes the raw JSON response, untouched, to `/opt/airflow/data/bronze/flights_<UTC timestamp>.json`.
- Pushes the file path to XCom under the key `bronze_file`, so every run produces a uniquely timestamped, traceable raw snapshot.

### 2. Silver — Cleaning & Transformation (`scripts/silver_transform.py`)

- Pulls the Bronze file path from XCom (`task_ids="bronze_ingest"`), with an explicit check that the path isn't empty — fails loudly instead of silently if the upstream task didn't produce output.
- Loads the JSON, extracts the `states` array (ignoring the top-level `time` key), and loads it into a Pandas DataFrame.
- Assigns the full 17-column OpenSky schema (`icao24`, `callsign`, `origin_country`, `longitude`, `latitude`, `velocity`, `on_ground`, etc.), then narrows the DataFrame down to the four columns the pipeline actually needs: `icao24`, `origin_country`, `velocity`, `on_ground`.
- Writes the cleaned data to `/opt/airflow/data/silver/flights_silver_<execution_date>.csv` and pushes the path to XCom under `silver_file`.

### 3. Gold — Aggregation (`scripts/gold_aggregate.py`)

- Pulls the Silver CSV path from XCom and loads it into a DataFrame.
- Groups by `origin_country` and aggregates:
  - `total_flights` — count of unique `icao24` identifiers
  - `avg_velocity` — mean velocity
  - `on_ground` — sum of grounded aircraft
- This collapses many per-aircraft rows into **one business-ready row per country per run**, suitable for direct consumption by a BI tool.
- Writes the result to the Gold path (derived by string-replacing `silver` → `gold` in the file path) and pushes it to XCom under `gold_file`.

### 4. Load — Snowflake Upsert (`scripts/load_gold_to_snowflake.py`)

- Pulls the Gold CSV path from XCom and reads it into a DataFrame.
- Authenticates to Snowflake using an Airflow **Connection** (`flight_snowflake`), retrieved via `BaseHook.get_connection()` — credentials are never hardcoded in the DAG or scripts.
- Connects using `snowflake.connector`, pulling account, warehouse, database, and role out of the connection's `extra` JSON field.
- For each row, executes a parameterized `MERGE INTO FLIGHT_KPIS` statement keyed on `(WINDOW_START, ORIGIN_COUNTRY)`:
  - **Matched** → updates `TOTAL_FLIGHTS`, `AVG_VELOCITY`, `ON_GROUND`, and refreshes `LOAD_TIME`.
  - **Not matched** → inserts a new row.
- `MERGE` was used deliberately over a plain `INSERT` — it prevents duplicate rows from accumulating if the DAG is re-run or backfilled for the same time window.
- The `WINDOW_START` timestamp comes from Airflow's `data_interval_start` (the start of the DAG's scheduled interval), not wall-clock time — so each row is tied to the business time window it represents, not the moment the task happened to execute.

---

## Infrastructure

The stack is defined in `docker-compose.yml` and runs four services:

| Service              | Image                  | Role                                                             |
|----------------------|-------------------------|-------------------------------------------------------------------|
| `postgres`           | `postgres:15`           | Airflow's metadata database                                       |
| `airflow-init`       | `apache/airflow:2.9.3`  | One-time setup: `airflow db init/migrate` + admin user creation   |
| `airflow-webserver`  | `apache/airflow:2.9.3`  | Airflow UI, served on `localhost:8080`                             |
| `airflow-scheduler`  | `apache/airflow:2.9.3`  | Parses DAGs and schedules/executes tasks (local executor)          |

- All Airflow services mount `./dags`, `./scripts`, `./logs`, `./plugins`, and `./data` as volumes — this is the **only** filesystem Airflow's containers can read from or write to.
- Credentials (Postgres user/password/db, Airflow admin user/email/password) are injected via a `.env` file at the project root rather than hardcoded in `docker-compose.yml`, following standard practice for code that's version-controlled.
- Executor: **LocalExecutor** — no separate worker or DAG-processor containers, since the scheduler alone comfortably handles this workload at a 30-minute cadence.

---

## Snowflake setup

Before the `load_gold_to_snowflake` task can run, the following must exist in Snowflake:

```sql
CREATE DATABASE IF NOT EXISTS FLIGHTS;
CREATE SCHEMA IF NOT EXISTS FLIGHTS.KPI;

CREATE TABLE IF NOT EXISTS FLIGHTS.KPI.FLIGHT_KPIS (
    WINDOW_START    TIMESTAMP,
    ORIGIN_COUNTRY  STRING,
    TOTAL_FLIGHTS   INTEGER,
    AVG_VELOCITY    FLOAT,
    ON_GROUND       INTEGER,
    LOAD_TIME       TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (WINDOW_START, ORIGIN_COUNTRY)
);
```

An Airflow **Connection** named `flight_snowflake` (type: Snowflake) must also be configured with the account, warehouse, database, schema, role, username, and password — set up once via the Airflow UI (**Admin → Connections**), not in code.

---

## Dashboard

Once data is flowing into `FLIGHT_KPIS`, a simple dashboard was built directly in **Snowflake**:
- A bar chart of countries ranked by total on-ground flights (`ORDER BY ON_GROUND DESC LIMIT 5`).
- A scorecard (KPI tile) showing the sum of grounded aircraft across all countries.

Since Gold-layer data is also saved as plain CSV, it can alternatively be connected directly to Power BI or Tableau without the Snowflake step, if a lighter-weight setup is preferred.

---

## Project structure

```
flight-ops-airflow/
├── docker-compose.yml
├── requirements.txt
├── .env                        # not committed — holds Postgres/Airflow credentials
├── dags/
│   └── flight-pipeline.py      # DAG definition, task wiring, dependencies
├── scripts/
│   ├── bronze_ingest.py        # API pull → raw JSON
│   ├── silver_transform.py     # JSON → cleaned CSV
│   ├── gold_aggregate.py       # cleaned CSV → aggregated CSV
│   └── load_gold_to_snowflake.py  # aggregated CSV → Snowflake (MERGE)
└── data/
    ├── bronze/
    ├── silver/
    └── gold/
```

---

## Running it locally

1. Install Docker Desktop.
2. Create the folder structure above and place the files accordingly.
3. Create a `.env` file in the project root with:
   ```
   POSTGRES_USER=...
   POSTGRES_PASSWORD=...
   POSTGRES_DB=...
   AIRFLOW_ADMIN_USER=...
   AIRFLOW_ADMIN_FIRSTNAME=...
   AIRFLOW_ADMIN_LASTNAME=...
   AIRFLOW_ADMIN_EMAIL=...
   AIRFLOW_ADMIN_PASSWORD=...
   ```
4. From the project root:
   ```bash
   pip install -r requirements.txt
   docker compose up -d
   ```
5. Visit `http://localhost:8080`, log in with the Airflow admin credentials set above.
6. In **Admin → Connections**, add a Snowflake connection named `flight_snowflake` with your account details.
7. Unpause the `flights_ops_medallion_pipe` DAG and trigger a run.

---

## Notes & limitations

- This is a learning/portfolio project, not a production system — it uses Airflow's `LocalExecutor` with no worker pool, no DAG tagging, and no retry backoff tuning beyond the defaults (`retries: 0`).
- The OpenSky public API has fair-use rate limits; this pipeline's 30-minute interval stays comfortably within them.
- Only four of the OpenSky API's 17 available fields are currently used downstream (`icao24`, `origin_country`, `velocity`, `on_ground`) — more columns are available in Silver if deeper analysis is needed later.
- Credentials are kept out of version control via `.env` and Airflow Connections — nothing sensitive is hardcoded in the DAG or task scripts.
