"""
Database connection and utility functions.
"""

import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from typing import Optional
import logging

logger = logging.getLogger(__name__)


def get_db_connection_string() -> str:
    """
    Construct database connection string from environment variables.
    
    Reads database configuration from environment variables with defaults
    for local development. In Docker, POSTGRES_HOST should be 'postgres' (service name).
    
    Returns:
        SQLAlchemy PostgreSQL connection string in format:
        postgresql://user:password@host:port/database
    """
    # Check if running in Docker (Airflow sets POSTGRES_HOST=postgres)
    # Otherwise use localhost for local development
    host = os.getenv('POSTGRES_HOST', 'localhost')
    port = os.getenv('POSTGRES_PORT', '5432')
    database = os.getenv('POSTGRES_DB', 'saas_analytics')
    user = os.getenv('POSTGRES_USER', 'postgres')
    password = os.getenv('POSTGRES_PASSWORD', 'postgres')
    
    # Log connection details (without password) for debugging
    logger.info(f"Connecting to database: {user}@{host}:{port}/{database}")
    
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


def create_db_engine() -> Engine:
    """
    Create SQLAlchemy engine for database connections.
    
    Configures connection pooling with pre-ping to verify connections
    before use, preventing stale connection errors.
    
    Returns:
        SQLAlchemy Engine instance configured for PostgreSQL
    """
    connection_string = get_db_connection_string()
    engine = create_engine(
        connection_string,
        pool_pre_ping=True,  # Verify connections before using
        pool_size=5,
        max_overflow=10
    )
    logger.info(f"Database engine created for {connection_string.split('@')[1]}")
    return engine


def initialize_warehouse_tables(engine: Engine, schemas: dict) -> None:
    """
    Initialize warehouse tables from schema definitions.
    
    Creates tables and indexes as defined in the schemas dictionary.
    Each schema SQL may contain multiple statements separated by semicolons.
    
    Args:
        engine: SQLAlchemy engine instance
        schemas: Dictionary mapping table names to CREATE TABLE statements
        
    Raises:
        Exception: If table creation fails
    """
    with engine.connect() as conn:
        for table_name, schema_sql in schemas.items():
            try:
                # Execute all statements in the schema (table + indexes)
                for statement in schema_sql.strip().split(';'):
                    statement = statement.strip()
                    if statement:
                        conn.execute(text(statement))
                conn.commit()
                logger.info(f"Table '{table_name}' initialized successfully")
            except Exception as e:
                logger.error(f"Error initializing table '{table_name}': {e}")
                conn.rollback()
                raise


def migrate_user_events_table(engine: Engine) -> None:
    """
    Migrate user_events table to add enrichment columns.
    
    Adds country, signup_source, and company_size columns if they don't exist.
    
    Args:
        engine: SQLAlchemy engine instance
    """
    migration_sql = """
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                      WHERE table_name='user_events' AND column_name='country') THEN
            ALTER TABLE user_events ADD COLUMN country VARCHAR(100);
        END IF;
        
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                      WHERE table_name='user_events' AND column_name='signup_source') THEN
            ALTER TABLE user_events ADD COLUMN signup_source VARCHAR(50);
        END IF;
        
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                      WHERE table_name='user_events' AND column_name='company_size') THEN
            ALTER TABLE user_events ADD COLUMN company_size VARCHAR(50);
        END IF;
    END $$;
    """
    
    with engine.connect() as conn:
        try:
            conn.execute(text(migration_sql))
            conn.commit()
            logger.info("User events table migration completed")
        except Exception as e:
            logger.error(f"Error migrating user_events table: {e}")
            conn.rollback()
            raise
