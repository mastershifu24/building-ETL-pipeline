"""
Main ETL pipeline entry point.

Orchestrates the complete ETL process: extraction from source files,
transformation and cleaning, and loading into the data warehouse.
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Add src to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Load environment variables from project root
# Don't override existing env vars (Docker sets POSTGRES_HOST=postgres)
load_dotenv(dotenv_path=project_root / '.env', override=False)

from src.utils.logger import setup_logger
from src.utils.database import create_db_engine
from src.extract.extractors import (
    extract_user_events,
    extract_subscriptions,
    extract_transactions,
    extract_user_profiles
)
from src.transform.transformers import (
    clean_user_events,
    clean_subscriptions,
    clean_transactions,
    clean_user_profiles,
    enrich_user_events
)
from src.load.loaders import (
    load_user_events,
    load_subscriptions,
    load_transactions,
    load_user_profiles
)

# Setup logger
logger = setup_logger(
    name="etl_pipeline",
    log_level=os.getenv('LOG_LEVEL', 'INFO'),
    log_path=os.getenv('LOG_PATH')
)


def run_etl_pipeline() -> None:
    """
    Run the complete ETL pipeline.
    
    This function orchestrates the three main phases of an ETL pipeline:
    1. EXTRACT: Read data from source files (JSON)
    2. TRANSFORM: Clean, validate, and enrich the data
    3. LOAD: Write the processed data to the PostgreSQL data warehouse
    
    The pipeline processes four main data types:
    - User Events: User interactions and feature usage
    - Subscriptions: Customer subscription information
    - Transactions: Payment and billing records
    - User Profiles: User demographic and account data
    
    Logs progress and summary statistics. Exits with code 1 on failure.
    """
    logger.info("=" * 60)
    logger.info("Starting ETL Pipeline")
    logger.info("=" * 60)
    
    try:
        # ============================================================
        # SETUP: Initialize database connection and configure paths
        # ============================================================
        # Create a connection to PostgreSQL using SQLAlchemy engine
        # This engine handles connection pooling and SQL execution
        engine = create_db_engine()
        
        # Determine where source data files are located
        # Supports both environment variable override and default location
        project_root = Path(__file__).parent.parent
        env_data_path = os.getenv('DATA_RAW_PATH')
        if env_data_path:
            # If env var is set and is relative, make it relative to project root
            env_path = Path(env_data_path)
            if not env_path.is_absolute():
                data_raw_path = str(project_root / env_data_path)
            else:
                data_raw_path = env_data_path
        else:
            data_raw_path = str(project_root / 'data' / 'raw')
        
        # Debug: log the path being used
        logger.info(f"Data raw path: {data_raw_path}")
        logger.info(f"Path exists: {Path(data_raw_path).exists()}")
        if Path(data_raw_path).exists():
            logger.info(f"Files in directory: {list(Path(data_raw_path).glob('*.json'))}")
        
        # ============================================================
        # PHASE 1: EXTRACT - Read data from source files
        # ============================================================
        # Extract raw data from JSON files into pandas DataFrames
        # Each extractor function reads a specific file and returns a DataFrame
        logger.info("\nEXTRACT PHASE")
        logger.info("-" * 60)
        
        user_events_raw = extract_user_events(f"{data_raw_path}/user_events.json")
        subscriptions_raw = extract_subscriptions(f"{data_raw_path}/subscriptions.json")
        transactions_raw = extract_transactions(f"{data_raw_path}/transactions.json")
        user_profiles_raw = extract_user_profiles(f"{data_raw_path}/user_profiles.json")
        
        # ============================================================
        # PHASE 2: TRANSFORM - Clean, validate, and enrich data
        # ============================================================
        # Clean each dataset:
        # - Convert data types (e.g., strings to dates)
        # - Remove duplicates
        # - Handle missing values
        # - Validate required fields
        # - Standardize formats (e.g., lowercase event types)
        logger.info("\nTRANSFORM PHASE")
        logger.info("-" * 60)
        
        user_events_clean = clean_user_events(user_events_raw)
        subscriptions_clean = clean_subscriptions(subscriptions_raw)
        transactions_clean = clean_transactions(transactions_raw)
        user_profiles_clean = clean_user_profiles(user_profiles_raw)
        
        # Enrichment: Add user profile data to events for better analytics
        # This joins user demographic data (country, signup_source, company_size)
        # to each event, enabling richer analysis later
        user_events_enriched = enrich_user_events(user_events_clean, user_profiles_clean)
        
        # ============================================================
        # PHASE 3: LOAD - Write data to data warehouse
        # ============================================================
        # Load each cleaned dataset into PostgreSQL tables
        # The loaders handle:
        # - Checking for existing records (to avoid duplicates)
        # - Batch inserts for performance
        # - Error handling and logging
        logger.info("\nLOAD PHASE")
        logger.info("-" * 60)
        
        rows_loaded = {
            'user_profiles': load_user_profiles(user_profiles_clean, engine),
            'subscriptions': load_subscriptions(subscriptions_clean, engine),
            'transactions': load_transactions(transactions_clean, engine),
            'user_events': load_user_events(user_events_enriched, engine)
        }
        
        # ============================================================
        # SUMMARY: Report results
        # ============================================================
        logger.info("\n" + "=" * 60)
        logger.info("ETL Pipeline Complete")
        logger.info("=" * 60)
        logger.info("\nRows loaded:")
        for table, count in rows_loaded.items():
            logger.info(f"  - {table}: {count:,} rows")
        
        logger.info("\nPipeline execution completed successfully.")
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    run_etl_pipeline()
