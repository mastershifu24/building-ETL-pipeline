"""
Airflow DAG for the SaaS analytics ETL pipeline.

This DAG defines a workflow that runs daily to:
1. Check that source data files exist
2. Execute the ETL pipeline (extract, transform, load)
3. Validate data quality after loading

Airflow will automatically schedule this DAG to run daily based on
the schedule_interval. You can also trigger it manually from the
Airflow UI at http://localhost:8080
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import sys
from pathlib import Path

# Add project root to Python path so Airflow can import our modules
# This allows the DAG to access functions from src/ and scripts/
# In Airflow container, src/ and scripts/ are mounted at /opt/airflow/
dag_dir = Path(__file__).parent  # /opt/airflow/dags
airflow_home = dag_dir.parent     # /opt/airflow
# Add /opt/airflow to Python path so we can import src and scripts
sys.path.insert(0, str(airflow_home))

# Import the functions that will be called by Airflow tasks
from src.main import run_etl_pipeline
from scripts.data_quality_check import check_data_quality

# Default arguments applied to all tasks in this DAG
default_args = {
    'owner': 'data_engineering',           # Task owner (for UI display)
    'depends_on_past': False,               # Don't require previous run to succeed
    'email_on_failure': False,              # Disable email alerts (set True in production)
    'email_on_retry': False,                # Disable email on retry
    'retries': 2,                           # Retry failed tasks up to 2 times
    'retry_delay': timedelta(minutes=5),    # Wait 5 minutes between retries
}

# Define the DAG (Directed Acyclic Graph)
dag = DAG(
    'saas_analytics_etl',                    # Unique DAG identifier
    default_args=default_args,               # Apply default args to all tasks
    description='Daily ETL pipeline for SaaS analytics',
    schedule_interval=timedelta(days=1),    # Run once per day
    start_date=datetime(2024, 1, 1),        # When DAG becomes active
    catchup=False,                          # Don't backfill past dates
    tags=['etl', 'analytics', 'saas'],      # Tags for filtering in UI
)

# ============================================================
# TASK 1: Check Data Availability
# ============================================================
# This task validates that all required source data files exist
# before attempting to run the ETL pipeline. This prevents
# the pipeline from failing halfway through if a file is missing.
check_data = BashOperator(
    task_id='check_data_availability',
    bash_command='''
    # Data is mounted at /opt/airflow/data in the container
    DATA_PATH="/opt/airflow/data/raw"
    echo "Checking for data files in: $DATA_PATH"
    ls -la "$DATA_PATH" || echo "Directory does not exist"
    # Check that all four required files exist
    # If any file is missing, exit with code 1 (failure)
    if [ -f "$DATA_PATH/user_events.json" ] && \
       [ -f "$DATA_PATH/subscriptions.json" ] && \
       [ -f "$DATA_PATH/transactions.json" ] && \
       [ -f "$DATA_PATH/user_profiles.json" ]; then
        echo "All source data files found"
        exit 0
    else
        echo "Missing required data files"
        exit 1
    fi
    ''',
    dag=dag,
)

# ============================================================
# TASK 2: Run ETL Pipeline
# ============================================================
# This is the main ETL task that:
# - Extracts data from source files
# - Transforms and cleans the data
# - Loads data into PostgreSQL warehouse
run_etl = PythonOperator(
    task_id='run_etl_pipeline',
    python_callable=run_etl_pipeline,  # Function to execute
    dag=dag,
)

# ============================================================
# TASK 3: Data Quality Check
# ============================================================
# After loading, validate that:
# - Records were loaded successfully
# - No null values in required fields
# - No duplicate primary keys
# - Referential integrity is maintained
# - Dates are consistent
# If any check fails, this task fails and Airflow marks the DAG as failed
data_quality_check = PythonOperator(
    task_id='data_quality_check',
    python_callable=check_data_quality,  # Function to execute
    dag=dag,
)

# ============================================================
# TASK DEPENDENCIES
# ============================================================
# Define the execution order:
# 1. First check data availability
# 2. Then run ETL (only if check passes)
# 3. Finally validate data quality (only if ETL succeeds)
# The >> operator means "runs after"
check_data >> run_etl >> data_quality_check
