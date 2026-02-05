"""
Database initialization script.

Creates warehouse tables and indexes as defined in the schema definitions.
"""

import os
import sys
from pathlib import Path

# Add src to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
from src.utils.database import (
    create_db_engine, 
    initialize_warehouse_tables,
    migrate_user_events_table
)
from src.models.schemas import WAREHOUSE_SCHEMAS
from src.utils.logger import setup_logger

# Load environment variables from project root
env_path = project_root / '.env'
load_dotenv(dotenv_path=env_path, override=True)

logger = setup_logger(log_level="DEBUG")
logger.info(f"Loading .env from: {env_path}")
logger.info(f".env file exists: {env_path.exists()}")


def main():
    """
    Initialize database schema.
    
    Creates all warehouse tables and indexes as defined in WAREHOUSE_SCHEMAS.
    Exits with error code 1 if initialization fails.
    """
    logger.info("Initializing database...")
    
    # Debug: verify environment variables are loaded
    logger.info(f"POSTGRES_HOST: {os.getenv('POSTGRES_HOST', 'NOT SET')}")
    logger.info(f"POSTGRES_USER: {os.getenv('POSTGRES_USER', 'NOT SET')}")
    logger.info(f"POSTGRES_DB: {os.getenv('POSTGRES_DB', 'NOT SET')}")
    password_set = os.getenv('POSTGRES_PASSWORD')
    logger.info(f"POSTGRES_PASSWORD: {'SET (length: ' + str(len(password_set)) + ')' if password_set else 'NOT SET'}")
    logger.info(f"POSTGRES_PASSWORD value: {password_set}")
    
    try:
        engine = create_db_engine()
        initialize_warehouse_tables(engine, WAREHOUSE_SCHEMAS)
        # Run migrations for existing tables
        migrate_user_events_table(engine)
        logger.info("Database initialization complete.")
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
