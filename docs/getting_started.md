# Getting Started Guide

## Prerequisites

- Python 3.9+
- Docker & Docker Compose
- Git

## Setup Instructions

### Step 1: Set Up Environment

```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# Windows:
venv\Scripts\activate
# Mac/Linux:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Step 2: Configure Environment

```bash
# Copy example environment file
copy .env.example .env  # Windows
# cp .env.example .env  # Mac/Linux

# Edit .env file with your settings (defaults should work for local development)
```

### Step 3: Start Infrastructure (Docker)

```bash
# Start PostgreSQL and Airflow
docker-compose up -d

# Wait for services to be ready (about 30-60 seconds)
# Check status:
docker-compose ps
```

### Step 4: Initialize Database

```bash
# Create warehouse tables
python scripts/setup_db.py
```

### Step 5: Generate Test Data

```bash
# Generate synthetic SaaS company data
python scripts/generate_data.py
```

This creates:
- 1,000 user profiles
- Subscription records
- Transaction data
- 10,000 user events

### Step 6: Run ETL Pipeline

```bash
# Run the pipeline
python src/main.py
```

### Step 7: Access Airflow UI (Optional)

```bash
# Airflow UI is available at:
# http://localhost:8080
# Username: airflow
# Password: airflow
```

## What This Pipeline Does

1. **Extracts** data from simulated sources (JSON files)
2. **Transforms** data (cleaning, validation, enrichment)
3. **Loads** data into PostgreSQL data warehouse
4. **Orchestrates** workflows with Airflow

## Troubleshooting

### Docker Issues
```bash
# Check if containers are running
docker-compose ps

# View logs
docker-compose logs

# Restart services
docker-compose restart
```

### Database Connection Issues
- Ensure PostgreSQL container is running
- Check `.env` file has correct credentials
- Verify port 5432 is not in use

### Import Errors
- Make sure virtual environment is activated
- Run `pip install -r requirements.txt` again
- Check Python version (3.9+)

## Learning Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Pandas Documentation](https://pandas.pydata.org/docs/)
