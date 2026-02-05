# Quick Reference: Commands to Run the Pipeline

## Prerequisites
Ensure Docker is running and containers are up:
```bash
docker-compose up -d
```

## Main Commands

### 1. Initialize Database Schema
```bash
python scripts/setup_db.py
```
Creates all tables in PostgreSQL with proper indexes and constraints.

### 2. Generate Test Data
```bash
python scripts/generate_data.py
```
Generates sample JSON files in `data/raw/`:
- `user_events.json` (10,000 events)
- `user_profiles.json` (1,000 profiles)
- `subscriptions.json` (varies)
- `transactions.json` (varies)

### 3. Run ETL Pipeline
```bash
python src/main.py
```
Executes the complete ETL process:
- Extracts data from `data/raw/` JSON files
- Transforms and cleans the data
- Loads into PostgreSQL data warehouse

### 4. Run Data Quality Checks
```bash
python scripts/data_quality_check.py
```
Validates data integrity:
- Record counts
- Null values
- Duplicates
- Referential integrity
- Date consistency

### 5. Query the Database
```bash
# Connect to PostgreSQL
docker exec -it saas_postgres psql -U postgres -d saas_analytics

# Run queries from scripts/queries.sql
# Or run individual queries:
SELECT COUNT(*) FROM user_events;
SELECT COUNT(*) FROM subscriptions;
\q  # Exit psql
```

### 6. Access Airflow UI
Open browser: http://localhost:8080
- Username: `airflow`
- Password: `airflow`

View and trigger DAGs from the web interface.

## Complete Workflow (First Time Setup)

```bash
# 1. Start Docker containers
docker-compose up -d

# 2. Initialize database
python scripts/setup_db.py

# 3. Generate test data
python scripts/generate_data.py

# 4. Run ETL pipeline
python src/main.py

# 5. Verify data quality
python scripts/data_quality_check.py

# 6. Query the data
docker exec -it saas_postgres psql -U postgres -d saas_analytics
```

## Troubleshooting

### Check Docker containers
```bash
docker-compose ps
```

### View container logs
```bash
docker-compose logs postgres
docker-compose logs airflow
```

### Restart containers
```bash
docker-compose restart
```

### Reset database (WARNING: deletes all data)
```bash
docker-compose down -v
docker-compose up -d
python scripts/setup_db.py
```
