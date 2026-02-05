"""
Data loading modules.

Provides functionality to load transformed data into the PostgreSQL
data warehouse with support for append and replace operations.
"""

import pandas as pd
from sqlalchemy.engine import Engine
import logging

logger = logging.getLogger(__name__)


def load_to_warehouse(
    df: pd.DataFrame,
    table_name: str,
    engine: Engine,
    if_exists: str = 'append',
    primary_key_col: str = None
) -> int:
    """
    Load DataFrame to PostgreSQL warehouse table.
    
    This function handles the final step of the ETL pipeline - writing
    transformed data to the data warehouse. It includes:
    - Duplicate prevention (checks existing records before inserting)
    - Batch processing (inserts in chunks for performance)
    - SQL injection prevention (validates table/column names)
    - Comprehensive error handling and logging
    
    Args:
        df: DataFrame containing cleaned, transformed data to load
        table_name: Name of the target PostgreSQL table
        engine: SQLAlchemy database engine (handles connections)
        if_exists: Behavior if table exists:
            - 'fail': Raise error if table exists
            - 'replace': Drop table and recreate (loses existing data)
            - 'append': Add new rows (default, used for incremental loads)
        primary_key_col: Column name to use for duplicate checking
            If provided, queries existing records and filters them out
            before inserting. This prevents duplicate key errors.
    
    Returns:
        Number of rows actually inserted (after duplicate filtering)
    
    Raises:
        ValueError: If table_name or primary_key_col contains invalid characters
        Exception: If database operation fails
    """
    # Early return if no data to load
    if df.empty:
        logger.warning(f"No data to load to {table_name}")
        return 0
    
    # ============================================================
    # SQL INJECTION PREVENTION
    # ============================================================
    # Validate table name contains only safe characters
    # This prevents SQL injection even though values are hardcoded in our code
    # It's a best practice to validate all dynamic SQL components
    if not table_name.replace('_', '').isalnum():
        raise ValueError(f"Invalid table name: {table_name}")
    
    try:
        # ============================================================
        # DUPLICATE PREVENTION
        # ============================================================
        # If primary key column is specified, check for existing records
        # This allows the pipeline to be run multiple times safely
        # (idempotent operation - same result whether run once or many times)
        if primary_key_col and primary_key_col in df.columns:
            try:
                # Validate column name for safety
                if not primary_key_col.replace('_', '').isalnum():
                    raise ValueError(f"Invalid primary key column name: {primary_key_col}")
                
                # Query existing primary keys from the table
                # Safe to use f-string here since both values are validated above
                # and are hardcoded in our loader functions (not user input)
                existing_query = f"SELECT {primary_key_col} FROM {table_name}"
                existing_df = pd.read_sql(existing_query, engine)
                
                # If there are existing records, filter them out
                if not existing_df.empty:
                    initial_count = len(df)
                    # Keep only rows where primary_key_col value is NOT in existing records
                    df = df[~df[primary_key_col].isin(existing_df[primary_key_col])]
                    filtered_count = initial_count - len(df)
                    if filtered_count > 0:
                        logger.info(f"Filtered out {filtered_count} existing records from {table_name}")
                        
            except Exception as e:
                # Table might not exist yet (first run), which is fine
                # Log at debug level since this is expected on first run
                logger.debug(f"Could not check existing records in {table_name}: {e}")
        
        # If all records were filtered out (already exist), skip insert
        if df.empty:
            logger.info(f"All records already exist in {table_name}, skipping insert")
            return 0
        
        # ============================================================
        # BATCH INSERT
        # ============================================================
        # Store count before insert (for logging)
        rows_loaded = len(df)
        
        # Use pandas to_sql for efficient bulk insert
        df.to_sql(
            table_name,           # Target table name
            engine,               # Database connection
            if_exists=if_exists,  # What to do if table exists
            index=False,          # Don't write DataFrame index as a column
            method='multi',       # Use multi-row INSERT for performance
            chunksize=100         # Insert 100 rows at a time (balances speed and memory)
        )
        
        logger.info(f"Loaded {rows_loaded} rows to {table_name}")
        return rows_loaded
        
    except Exception as e:
        logger.error(f"Error loading data to {table_name}: {e}")
        raise  # Re-raise to allow caller to handle


def load_user_events(df: pd.DataFrame, engine: Engine) -> int:
    """
    Load user events to warehouse.
    
    Args:
        df: User events DataFrame
        engine: Database engine
    
    Returns:
        Number of rows loaded
    """
    return load_to_warehouse(df, 'user_events', engine, primary_key_col='event_id')


def load_subscriptions(df: pd.DataFrame, engine: Engine) -> int:
    """
    Load subscriptions to warehouse.
    
    Args:
        df: Subscriptions DataFrame
        engine: Database engine
    
    Returns:
        Number of rows loaded
    """
    return load_to_warehouse(df, 'subscriptions', engine, primary_key_col='subscription_id')


def load_transactions(df: pd.DataFrame, engine: Engine) -> int:
    """
    Load transactions to warehouse.
    
    Args:
        df: Transactions DataFrame
        engine: Database engine
    
    Returns:
        Number of rows loaded
    """
    return load_to_warehouse(df, 'transactions', engine, primary_key_col='transaction_id')


def load_user_profiles(df: pd.DataFrame, engine: Engine) -> int:
    """
    Load user profiles to warehouse.
    
    Args:
        df: User profiles DataFrame
        engine: Database engine
    
    Returns:
        Number of rows loaded
    """
    # Use replace for user_profiles to handle updates
    if df.empty:
        logger.warning("No user profiles to load")
        return 0
    
    try:
        rows_loaded = len(df)
        df.to_sql(
            'user_profiles',
            engine,
            if_exists='replace',  # Replace to handle profile updates
            index=False,
            method='multi',
            chunksize=100  # Reduced to avoid parameter limit issues
        )
        logger.info(f"Loaded {rows_loaded} user profiles to warehouse")
        return rows_loaded
    except Exception as e:
        logger.error(f"Error loading user profiles: {e}")
        raise
