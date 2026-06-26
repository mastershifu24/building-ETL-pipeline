"""
Main ETL pipeline entry point.

Orchestrates the complete ETL process: extraction from source files,
transformation and cleaning, and loading into the data warehouse.

User events are processed in chunks when sourced from Parquet so the
pipeline can handle 1M+ rows without loading everything into memory.
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
    extract_subscriptions,
    extract_transactions,
    extract_user_profiles,
    iter_user_events_chunks,
    resolve_user_events_path,
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


def _resolve_data_raw_path() -> str:
    env_data_path = os.getenv('DATA_RAW_PATH')
    if env_data_path:
        env_path = Path(env_data_path)
        if not env_path.is_absolute():
            return str(project_root / env_data_path)
        return env_data_path
    return str(project_root / 'data' / 'raw')


def _load_user_events_chunked(
    data_raw_path: str,
    user_profiles_clean,
    engine,
    chunk_size: int
) -> int:
    """
    Transform and load user events in chunks.

    Each chunk follows: extract chunk -> clean -> enrich -> staging upsert.
    """
    events_path = resolve_user_events_path(data_raw_path)
    total_loaded = 0
    chunk_number = 0

    logger.info(
        f"Processing user events from {events_path} in chunks of {chunk_size:,}"
    )

    for events_chunk in iter_user_events_chunks(str(events_path), chunk_size=chunk_size):
        chunk_number += 1
        logger.info(f"User events chunk {chunk_number}: {len(events_chunk):,} rows")

        events_clean = clean_user_events(events_chunk)
        events_enriched = enrich_user_events(events_clean, user_profiles_clean)
        total_loaded += load_user_events(events_enriched, engine)

    if chunk_number == 0:
        logger.warning("No user events found to process")

    return total_loaded


def run_etl_pipeline() -> None:
    """
    Run the complete ETL pipeline.
    
    This function orchestrates the three main phases of an ETL pipeline:
    1. EXTRACT: Read data from source files (JSON/Parquet)
    2. TRANSFORM: Clean, validate, and enrich the data
    3. LOAD: Write the processed data to the PostgreSQL data warehouse
    
    The pipeline processes four main data types:
    - User Events: User interactions and feature usage (chunked at scale)
    - Subscriptions: Customer subscription information
    - Transactions: Payment and billing records
    - User Profiles: User demographic and account data
    
    Logs progress and summary statistics. Exits with code 1 on failure.
    """
    logger.info("=" * 60)
    logger.info("Starting ETL Pipeline")
    logger.info("=" * 60)
    
    try:
        engine = create_db_engine()
        data_raw_path = _resolve_data_raw_path()
        chunk_size = int(os.getenv('ETL_CHUNK_SIZE', '50000'))

        logger.info(f"Data raw path: {data_raw_path}")
        logger.info(f"Path exists: {Path(data_raw_path).exists()}")
        if Path(data_raw_path).exists():
            logger.info(
                f"Files in directory: {list(Path(data_raw_path).glob('*events*'))}"
            )

        # ============================================================
        # PHASE 1: EXTRACT - dimension tables (small, load whole file)
        # ============================================================
        logger.info("\nEXTRACT PHASE")
        logger.info("-" * 60)

        subscriptions_raw = extract_subscriptions(f"{data_raw_path}/subscriptions.json")
        transactions_raw = extract_transactions(f"{data_raw_path}/transactions.json")
        user_profiles_raw = extract_user_profiles(f"{data_raw_path}/user_profiles.json")

        # ============================================================
        # PHASE 2: TRANSFORM - dimension tables
        # ============================================================
        logger.info("\nTRANSFORM PHASE")
        logger.info("-" * 60)

        subscriptions_clean = clean_subscriptions(subscriptions_raw)
        transactions_clean = clean_transactions(transactions_raw)
        user_profiles_clean = clean_user_profiles(user_profiles_raw)

        # ============================================================
        # PHASE 3: LOAD
        # ============================================================
        logger.info("\nLOAD PHASE")
        logger.info("-" * 60)

        rows_loaded = {
            'user_profiles': load_user_profiles(user_profiles_clean, engine),
            'subscriptions': load_subscriptions(subscriptions_clean, engine),
            'transactions': load_transactions(transactions_clean, engine),
            'user_events': _load_user_events_chunked(
                data_raw_path,
                user_profiles_clean,
                engine,
                chunk_size
            ),
        }

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
