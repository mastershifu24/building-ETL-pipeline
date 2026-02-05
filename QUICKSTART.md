# Quick Start Guide

## Prerequisites Check

Before starting, ensure you have:
- ✅ Python 3.9 or higher (`python --version`)
- ✅ Docker Desktop installed and running
- ✅ Docker Compose available (`docker-compose --version`)

## Step-by-Step Execution

### 1. Set Up Python Environment

```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# Windows PowerShell:
.\venv\Scripts\Activate.ps1
# Windows CMD:
venv\Scripts\activate.bat
# Mac/Linux:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure Environment Variables

```bash
# Windows:
copy .env.example .env

# Mac/Linux:
cp .env.example .env
```

The default `.env` values work for local development. No changes needed unless you customize ports or credentials.

### 3. Start Infrastructure (Docker)

```bash
# Start PostgreSQL and Airflow containers
docker-compose up -d

# Wait 30-60 seconds for services to initialize
# Check status:
docker-compose ps
```

You should see:
- `saas_postgres` - running
- `saas_airflow_webserver` - running  
- `saas_airflow_scheduler` - running

### 4. Initialize Database Schema

```bash
# Create warehouse tables
python scripts/setup_db.py
```

Expected output:
```
INFO - Database engine created for localhost:5432/saas_analytics
INFO - Table 'user_events' initialized successfully
INFO - Table 'subscriptions' initialized successfully
INFO - Table 'transactions' initialized successfully
INFO - Table 'user_profiles' initialized successfully
INFO - Database initialization complete.
```

### 5. Generate Test Data

```bash
# Generate synthetic SaaS company data
python scripts/generate_data.py
```

This creates JSON files in `data/raw/`:
- `user_profiles.json` (~1,000 records)
- `subscriptions.json` (~1,000-2,000 records)
- `transactions.json` (~500-1,000 records)
- `user_events.json` (~10,000 records)

### 6. Run ETL Pipeline

```bash
# Execute the complete ETL process
python src/main.py
```

Expected output:
```
============================================================
Starting ETL Pipeline
============================================================

EXTRACT PHASE
------------------------------------------------------------
INFO - Extracting data from data/raw/user_events.json
INFO - Extracted 10000 records from data/raw/user_events.json
...

TRANSFORM PHASE
------------------------------------------------------------
INFO - Cleaning 10000 user events
...

LOAD PHASE
------------------------------------------------------------
INFO - Loaded 1000 rows to user_profiles
...

============================================================
ETL Pipeline Complete
============================================================

Rows loaded:
  - user_profiles: 1,000 rows
  - subscriptions: 1,234 rows
  - transactions: 567 rows
  - user_events: 10,000 rows

Pipeline execution completed successfully.
```

### 7. Verify Data (Optional)

```bash
# Run data quality checks
python scripts/data_quality_check.py
```

Or query the database directly:
```bash
# Connect to PostgreSQL (from Docker)
docker exec -it saas_postgres psql -U postgres -d saas_analytics

# Run queries:
SELECT COUNT(*) FROM user_events;
SELECT COUNT(*) FROM subscriptions;
SELECT COUNT(*) FROM transactions;
SELECT COUNT(*) FROM user_profiles;
\q
```

### 8. Access Airflow UI (Optional)

Open your browser to: http://localhost:8080

- Username: `airflow`
- Password: `airflow`

You can view and trigger the DAG manually from the UI.

## Troubleshooting

### Issue: Docker containers won't start
```bash
# Check Docker Desktop is running
# Check ports 5432 and 8080 are not in use
# View logs:
docker-compose logs
```

### Issue: Database connection error
```bash
# Ensure PostgreSQL container is running
docker-compose ps

# Check .env file exists and has correct values
# Restart containers:
docker-compose restart postgres
```

### Issue: Import errors
```bash
# Ensure virtual environment is activated
# Reinstall dependencies:
pip install -r requirements.txt --upgrade
```

### Issue: No data files found
```bash
# Ensure you ran generate_data.py first
# Check files exist:
ls data/raw/*.json  # Mac/Linux
dir data\raw\*.json  # Windows
```

### Issue: Permission errors (Linux/Mac)
```bash
# Fix Airflow permissions
export AIRFLOW_UID=$(id -u)
docker-compose up -d
```

## Next Steps

- **Run via Airflow**: The DAG is available at http://localhost:8080
- **Modify data**: Edit `scripts/generate_data.py` to change data volume/types
- **Customize transformations**: Edit functions in `src/transform/transformers.py`
- **Add new data sources**: Extend `src/extract/extractors.py`

## Stopping Services

```bash
# Stop all containers
docker-compose down

# Stop and remove volumes (clears database)
docker-compose down -v
```
