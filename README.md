# B2B SaaS Analytics Pipeline

A complete data engineering project that transforms raw operational data into business intelligence for a B2B SaaS company.

## What This Project Does

This pipeline processes four core data streams that every SaaS company generates:

| Data Source | Business Question It Answers |
|-------------|------------------------------|
| **User Events** | What are customers actually doing in our product? |
| **User Profiles** | Who are our customers and how do we segment them? |
| **Subscriptions** | What's our MRR and who's at risk of churning? |
| **Transactions** | Is revenue being collected correctly? |

The pipeline extracts this data, validates it for quality, transforms it into analysis-ready tables, and loads it into a PostgreSQL data warehouse.

## Architecture

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

## Key Design Decisions

**Why these choices?** See [docs/design_decisions.md](docs/design_decisions.md) for detailed trade-off analysis.

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Warehouse | PostgreSQL | ACID compliance for financial data, free, scales to millions of rows |
| Processing | Batch (hourly/daily) | SaaS metrics are inherently batch; simpler than streaming |
| Modeling | Kimball Star Schema | Optimized for BI queries, business users understand it |
| Orchestration | Apache Airflow | Industry standard, handles dependencies and retries |
| Quality | Custom framework | Lightweight, inspired by Great Expectations patterns |

## Data Model

The warehouse uses dimensional modeling for analytics:

**Dimensions (the "who/what/when"):**
- `dim_account` - B2B companies (the paying entity)
- `dim_plan` - Subscription tiers (Free, Basic, Pro, Enterprise)
- `dim_date` - Pre-populated calendar for time intelligence

**Facts (the "how much/how many"):**
- `fact_subscription_daily` - Daily snapshot of subscription state and MRR
- `fact_user_events` - Individual user interactions

See [models/](models/) for SQL definitions with comments explaining each table.

## Quick Start

### Prerequisites
- Python 3.9+
- Docker & Docker Compose

### Setup

```bash
# 1. Clone and set up
cd "building ETL pipeline"
python -m venv venv
venv\Scripts\activate  # Windows
pip install -r requirements.txt

# 2. Configure environment
cp .env.example .env

# 3. Start PostgreSQL
docker-compose up -d

# 4. Initialize database and generate sample data
python scripts/setup_db.py
python scripts/generate_data.py

# 5. Run the pipeline
python src/main.py

# 6. Validate data quality
python scripts/data_quality_check.py
```

## Project Structure

```
├── src/
│   ├── extract/          # Data extraction from JSON files
│   ├── transform/        # Business logic transformations
│   ├── load/             # Database loading with upsert logic
│   ├── data_quality/     # Quality check framework
│   ├── models/           # Python dataclasses for schemas
│   └── main.py           # Pipeline orchestrator
│
├── models/               # SQL dimensional model definitions
│   ├── dim_account.sql
│   ├── dim_plan.sql
│   ├── dim_date.sql
│   ├── fact_subscriptions.sql
│   └── fact_user_events.sql
│
├── dags/                 # Airflow DAG definitions
├── scripts/              # Setup and utility scripts
├── docs/                 # Design documentation
│   ├── business_context.md
│   └── design_decisions.md
└── data/                 # Raw and processed data files
```

## Data Quality Framework

The pipeline includes a reusable quality framework (`src/data_quality/`) that validates:

- **Completeness**: Row counts meet minimums
- **Validity**: No nulls in required fields
- **Uniqueness**: Primary keys are unique
- **Integrity**: Foreign keys reference valid records
- **Freshness**: Data was loaded recently

Example usage:

```python
from src.data_quality import DataQualityChecker

result = (DataQualityChecker(engine, "My Suite")
    .expect_row_count("user_events", min_count=1000)
    .expect_no_nulls("subscriptions", ["subscription_id", "user_id"])
    .expect_unique("transactions", ["transaction_id"])
    .run())

if not result.success:
    raise ValueError(result.summary())
```

## Business Context

This platform is designed to answer core B2B SaaS questions:

- **What's our MRR?** → Query `fact_subscription_daily`
- **Which accounts are churning?** → Filter by `is_churned` flag
- **What features do retained customers use?** → Join events to non-churned accounts
- **How long until trials convert?** → Calculate `days_since_signup`

See [docs/business_context.md](docs/business_context.md) for metric definitions and example queries.

## Tech Stack

| Component | Technology |
|-----------|------------|
| Language | Python 3.9+ |
| Database | PostgreSQL 13+ |
| Orchestration | Apache Airflow 2.x |
| Data Processing | pandas |
| Containerization | Docker, Docker Compose |

## License

MIT License
