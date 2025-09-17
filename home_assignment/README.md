
# SpaceX Ingestion & Aggregation – README

This project ingests launch data into PostgreSQL and provides analytical SQL to aggregate and explore it (e.g., success rates, payload mass, delays, and site-level stats). It also includes an optional Trino setup for federated/interactive querying.

---

## Table of Contents
- [Architecture Overview](#architecture-overview)
- [Repository Layout](#repository-layout)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
  - [1) Start PostgreSQL (and Trino)](#1-start-postgresql-and-trino)
  - [2) (Optional) Wire Trino to Postgres](#2-optional-wire-trino-to-postgres)
  - [3) Install Python dependencies](#3-install-python-dependencies)
- [Design Choices & Assumptions](#design-choices--assumptions)
- [How to Run Ingestion](#how-to-run-ingestion)
- [How to Run Aggregations / Analytics](#how-to-run-aggregations--analytics)
  - [Creating a `stg` view (for the analytics SQL)](#creating-a-stg-view-for-the-analytics-sql)
  - [Run each analytic query](#run-each-analytic-query)
- [How to Test / Validate](#how-to-test--validate)
- [Troubleshooting](#troubleshooting)

---

## Architecture Overview

- **PostgreSQL**: primary storage for the raw launch data and dimensions.
- **Python (`api.py`)**: ingests data into PostgreSQL (raw table & dimensions).
- **SQL**: aggregation & analytics using standard SQL.
- **Trino (optional)**: fast, interactive SQL engine that can query the same Postgres data via a Trino catalog.



## Prerequisites

- **Docker** and **Docker Compose**
- **Python 3.9+** 

---

## Setup

### 1) Start PostgreSQL (and Trino)

From the repository root:

```bash
docker compose up -d
```

This launches:
- **Postgres** on `localhost:5432` with `user=kobi`, `password=kobi`, `db=test`
- **Trino** on `localhost:8080` (after Postgres is healthy)

### 2) (Optional) Wire Trino to Postgres


### 3) Install Python dependencies

Create/activate a virtualenv (optional but recommended), then install:

```bash
pip install -r requirements.txt
```

---

## Design Choices & Assumptions

- **Raw & Dimensions**  
  - The ingestion creates/uses a **`public.raw_level`** table for launch rows (containing fields like `id`, `date_utc`, `static_fire_date_utc`, `success`, `payloads` array, etc.).  
  - Dimension tables **`dim_payloads`** and **`dim_launchpads`** store payload metadata and launchpad details. The analytics SQL expects them to exist.
- **Payloads as arrays**  
  - The raw table stores `payloads` as an array; downstream analytics **UNNEST** this array to join with `dim_payloads` and compute totals per launch.
- **Delay calculation**  
  - For delay metrics, we compute hour differences only when `date_utc >= COALESCE(static_fire_date_utc, date_utc)`. If `static_fire_date_utc` is null, we treat delay as 0 for that row (filtering out invalid/negative cases). See `aggregated.sql` and `delay.sql` for the exact logic.
- **Incremental load**  
  - The incremental example demonstrates selecting new `id`s that do not already exist in `raw_level`. It shows both a **Trino-side** approach and a commented **DB-side insert** pattern.
- **Idempotency**  
  - Ingestion should be idempotent on `id` (unique identifier for a launch). Upserts/merges ensure re-runs don’t duplicate data.
- **Trino is optional**  
  - All analytics can run directly in Postgres; Trino is included for fast ad‑hoc querying and federated access if needed.

---

## How to Run Ingestion and Check

> Ensure Docker services are up and dependencies are installed.

Run the ingestion script:

```bash
python api.py
```

- The script connects to the Postgres DB defined in `docker-compose.yml` and populates `public.raw_level`, `dim_payloads`, and `dim_launchpads` as needed.
- Adjust environment variables or connection strings in `api.py` if your local setup differs.
- If you want to test specific function just run it in the main function