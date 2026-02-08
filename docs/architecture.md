# Architecture Overview

## The Big Picture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              DATA FLOW                                      │
└─────────────────────────────────────────────────────────────────────────────┘

   RAW DATA                    PROCESSING                      ANALYTICS
   (JSON files)                (Python ETL)                    (PostgreSQL)
                                                               
┌──────────────┐          ┌──────────────────┐          ┌──────────────────┐
│ user_events  │          │                  │          │   OPERATIONAL    │
│ .json        │─────────▶│    EXTRACT       │          │   TABLES         │
├──────────────┤          │  Read JSON files │          │                  │
│ user_profiles│          │  into DataFrames │          │  user_events     │
│ .json        │─────────▶│                  │─────────▶│  user_profiles   │
├──────────────┤          └────────┬─────────┘          │  subscriptions   │
│ subscriptions│                   │                    │  transactions    │
│ .json        │─────────▶         ▼                    └────────┬─────────┘
├──────────────┤          ┌──────────────────┐                   │
│ transactions │          │                  │                   │
│ .json        │─────────▶│   TRANSFORM      │                   ▼
└──────────────┘          │  Clean, validate │          ┌──────────────────┐
                          │  parse dates     │          │   DIMENSIONAL    │
                          │                  │          │   MODEL          │
                          └────────┬─────────┘          │                  │
                                   │                    │  dim_account     │
                                   ▼                    │  dim_plan        │
                          ┌──────────────────┐          │  dim_date        │
                          │                  │          │                  │
                          │     LOAD         │─────────▶│  fact_subs_daily │
                          │  Insert/Upsert   │          │  fact_user_events│
                          │  to PostgreSQL   │          │                  │
                          │                  │          └────────┬─────────┘
                          └──────────────────┘                   │
                                                                 ▼
                                                        ┌──────────────────┐
                                                        │   DASHBOARDS     │
                                                        │   & ANALYTICS    │
                                                        │                  │
                                                        │  "What's MRR?"   │
                                                        │  "Who churned?"  │
                                                        └──────────────────┘
```

## How Each Component Works

### 1. Raw Data (JSON Files)
**Location:** `data/raw/`

These simulate what you'd get from a production system:
- `user_events.json` - Clicks, page views, feature usage
- `user_profiles.json` - Who the users are
- `subscriptions.json` - What plan they're on
- `transactions.json` - Payment history

### 2. ETL Pipeline (Python)
**Location:** `src/`

```
src/
├── extract/extractors.py    → Reads JSON → pandas DataFrame
├── transform/transformers.py → Cleans data, validates, parses dates
├── load/loaders.py          → Writes DataFrame → PostgreSQL
├── data_quality/            → Validates after loading
└── main.py                  → Runs Extract → Transform → Load
```

**Key insight:** Each step is independent. If extract fails, transform doesn't run.

### 3. Orchestration (Airflow)
**Location:** `dags/`

Airflow answers: **"When does this run? What order? What if it fails?"**

```
┌─────────────────────┐     ┌─────────────────────┐     ┌─────────────────────┐
│ check_data_exists   │────▶│   run_etl_pipeline  │────▶│  data_quality_check │
│                     │     │                     │     │                     │
│ "Do files exist?"   │     │ "Run src/main.py"   │     │ "Is data valid?"    │
└─────────────────────┘     └─────────────────────┘     └─────────────────────┘
        │                           │                           │
        ▼                           ▼                           ▼
   If fails: STOP            If fails: RETRY 2x          If fails: ALERT
```

### 4. Data Warehouse (PostgreSQL)
**Location:** Database running in Docker

**Operational Tables** (raw data landing zone):
- Direct load from ETL
- Schema matches source data

**Dimensional Model** (analytics-ready):
- Star schema for fast queries
- Designed for business questions

```
                    ┌─────────────┐
                    │  dim_date   │
                    │  (2192 days)│
                    └──────┬──────┘
                           │
┌─────────────┐    ┌───────┴───────┐    ┌─────────────┐
│ dim_account │────│ fact_subs_   │────│  dim_plan   │
│ (1000 accts)│    │    daily     │    │ (4 plans)   │
└─────────────┘    │ (12K rows)   │    └─────────────┘
                   └──────────────┘
```

## Why This Architecture?

| Decision | Rationale |
|----------|-----------|
| **Batch, not streaming** | SaaS metrics (MRR, churn) are daily/monthly concepts |
| **PostgreSQL, not Snowflake** | Free, ACID-compliant, sufficient for this scale |
| **Star schema** | Optimized for BI queries, easy for analysts to understand |
| **Airflow** | Industry standard, handles retries and dependencies |
| **Python/pandas** | Flexible, testable, integrates with data science |

## One-Sentence Summary

> Raw JSON data flows through a Python ETL pipeline, orchestrated by Airflow, 
> into a PostgreSQL warehouse modeled as a star schema for SaaS analytics.
