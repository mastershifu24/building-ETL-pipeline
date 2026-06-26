# B2B SaaS Analytics Pipeline

A production-style **ETL and analytics** project: raw operational data → validated transforms → **PostgreSQL** warehouse, **dimensional models**, **data quality** checks, and **Airflow** orchestration. Built to mirror how B2B SaaS teams model subscriptions, events, and revenue for reporting.

**Stack:** Python · SQL · PostgreSQL · Neon · pandas · SQLAlchemy · GitHub Actions · Streamlit · Docker · Apache Airflow

**Live demo:** [Streamlit dashboard](https://share.streamlit.io) *(deploy via steps below)* · [GitHub Actions ETL](https://github.com/mastershifu24/building-ETL-pipeline/actions)

---

## What this repo demonstrates

| Area | What’s in the repo |
|------|--------------------|
| **ETL** | Extract (JSON) → clean/transform → load with idempotency and typed loads |
| **SQL modeling** | Kimball-style `dim_*` / `fact_*` definitions in [`models/`](models/) |
| **Data quality** | Reusable checks: row counts, nulls, uniqueness, referential integrity, freshness ([`src/data_quality/`](src/data_quality/)) |
| **Orchestration** | Airflow DAGs to schedule runs and quality gates ([`dags/`](dags/)) |
| **Ops** | Docker Compose for Postgres and Airflow, env-based config |

**Business questions the model supports (examples):** MRR, churn risk, user segmentation, payment consistency — see [docs/business_context.md](docs/business_context.md).

---

## Architecture (high level)

```
Raw Data (JSON)     ETL Pipeline (Python)     Data Warehouse (PostgreSQL)
     │                      │                          │
     ▼                      ▼                          ▼
┌──────────┐         ┌──────────────┐           ┌──────────────┐
│ accounts │─────────│   Extract    │           │ dim_account  │
│ users    │         │   Validate   │──────────▶│ dim_plan     │
│ events   │─────────│   Transform  │           │ dim_date     │
│ subs     │         │   Load       │           │ fact_*       │
└──────────┘         └──────────────┘           └──────────────┘
                            │
                            ▼
                     ┌──────────────┐
                     │   Airflow    │
                     │  (Schedule)  │
                     └──────────────┘
```

---

## Key design decisions

| Decision | Choice | Why |
|----------|--------|-----|
| Warehouse | PostgreSQL | Relational integrity, good fit for finance-style facts, easy to run locally |
| Batch | Daily/hourly-style runs | Aligns with typical SaaS reporting; simpler ops than streaming for this scope |
| Modeling | Star schema (Kimball) | Clear facts/dimensions for BI and ad hoc SQL |
| Orchestration | Airflow | Dependency graphs, retries, scheduled runs |
| Quality | Custom Python + SQL | Explicit checks; fail fast before dashboards trust bad data |

More detail: [docs/design_decisions.md](docs/design_decisions.md)

---

## Dashboard (no local ETL needed)

After [GitHub Actions](#cloud-etl-neon) loads data into Neon:

```bash
pip install -r requirements.txt
streamlit run dashboard/app.py
```

**Streamlit Community Cloud** (free, always on):

1. Push this repo to GitHub
2. Go to [share.streamlit.io](https://share.streamlit.io) → **Create app** → select this repo
3. Main file path: `dashboard/app.py`
4. **Advanced settings → Secrets** — paste your Neon `DATABASE_URL` (same as GitHub Actions secret)
5. Deploy — you get a public URL like `https://your-app.streamlit.app`

Data refreshes when the daily GitHub Actions workflow runs (or trigger it manually).

---

## Cloud ETL (Neon)

Scheduled pipeline: [`.github/workflows/etl-neon.yml`](.github/workflows/etl-neon.yml) runs daily against Neon. Add repo secret `DATABASE_URL`, then **Actions → ETL to Neon → Run workflow**.

---

## Quick start (from repo root)

**Prerequisites:** Python 3.9+, Docker Desktop running

```bash
# 1) Environment
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env

# 2) Database
docker-compose up -d

# 3) Schema + sample data
python scripts/setup_db.py
python scripts/generate_data.py

# 4) Run ETL (must be run from project root so paths resolve)
python src/main.py

# 5) Data quality
python scripts/data_quality_check.py
```

**Optional:** dimensional model — see [QUICKSTART.md](QUICKSTART.md), [scripts/init_dimensional_model.py](scripts/init_dimensional_model.py), [scripts/load_dimensional_model.py](scripts/load_dimensional_model.py).

**Troubleshooting**

- `Connection refused` to Postgres: start Docker, confirm `.env` host/port (e.g. `127.0.0.1` and the mapped port from `docker-compose.yml`).
- `File not found` for `data/raw/*`: run `generate_data.py` from the project root; run `main.py` from the project root, not from `src/`.
- Airflow tasks: use service hostname `postgres` inside containers, not `localhost`, per your `.env` for containerized runs.

---

## Project structure

```
├── src/
│   ├── extract/          # JSON → DataFrame
│   ├── transform/        # Cleaning, enrichment
│   ├── load/             # Warehouse loads
│   ├── data_quality/     # Check suite
│   └── main.py           # Pipeline entry point
├── models/               # SQL for dim/fact tables
├── dags/                 # Airflow DAGs
├── scripts/              # setup, generate, quality, queries
└── docs/                 # Business context, design notes, architecture
```

---

## Data quality (summary)

The pipeline can enforce completeness, null rules, PK uniqueness, referential integrity, and freshness. See [src/data_quality/](src/data_quality/) and [scripts/data_quality_check.py](scripts/data_quality_check.py).

---

## Tech stack

| Layer | Technology |
|--------|------------|
| Language | Python 3.9+ |
| Database | PostgreSQL |
| Orchestration | Apache Airflow 2.x |
| Data processing | pandas, SQLAlchemy |
| Runtime | Docker Compose |

---

## Documentation

- [docs/business_context.md](docs/business_context.md) — metrics and business framing  
- [docs/design_decisions.md](docs/design_decisions.md) — tradeoffs  
- [docs/architecture.md](docs/architecture.md) — system overview  

---

## License

MIT License

---

**Author** — [Ahmed Shifa](https://www.linkedin.com/in/ahmed-shifa/) · Open to **data engineering / analytics engineering** roles.  
*If this repo is useful, a star on GitHub helps visibility.*
