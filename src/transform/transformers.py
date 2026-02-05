"""
Data transformation modules.

Provides data cleaning, validation, enrichment, and transformation
functions for pipeline data processing.
"""

import pandas as pd
from typing import Optional
import logging
import json

logger = logging.getLogger(__name__)


def validate_dataframe(df: pd.DataFrame, required_columns: list) -> bool:
    """
    Validate that DataFrame has required columns.
    
    Args:
        df: DataFrame to validate
        required_columns: List of required column names
    
    Returns:
        True if valid, raises ValueError otherwise
    """
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    return True


def clean_user_events(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and transform user events data.
    
    This function performs data quality operations:
    - Validates required columns exist
    - Converts data types (strings to dates)
    - Removes duplicate records
    - Filters invalid data (null timestamps)
    - Standardizes formats (lowercase event types)
    - Serializes JSON properties for database storage
    
    Args:
        df: Raw user events DataFrame from extraction phase
    
    Returns:
        Cleaned DataFrame ready for loading into warehouse
    
    Raises:
        ValueError: If required columns are missing
    """
    logger.info(f"Cleaning {len(df)} user events")
    
    # Early return if no data to process
    if df.empty:
        return df
    
    # ============================================================
    # STEP 1: VALIDATION - Ensure required columns exist
    # ============================================================
    # This prevents errors later if source data structure changes
    required_cols = ['event_id', 'user_id', 'event_type', 'timestamp']
    validate_dataframe(df, required_cols)
    
    # ============================================================
    # STEP 2: DATA TYPE CONVERSION
    # ============================================================
    # Convert timestamp string to datetime object
    # This enables date filtering, sorting, and time-based analysis
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # ============================================================
    # STEP 3: DEDUPLICATION
    # ============================================================
    # Remove duplicate events based on event_id (primary key)
    # Keep the first occurrence, drop subsequent duplicates
    initial_count = len(df)
    df = df.drop_duplicates(subset=['event_id'], keep='first')
    if len(df) < initial_count:
        logger.warning(f"Removed {initial_count - len(df)} duplicate events")
    
    # ============================================================
    # STEP 4: DATA QUALITY FILTERING
    # ============================================================
    # Remove rows with invalid timestamps (null/NaN values)
    # Events without timestamps can't be analyzed temporally
    df = df[df['timestamp'].notna()]
    
    # ============================================================
    # STEP 5: STANDARDIZATION
    # ============================================================
    # Convert event_type to lowercase for consistency
    # This prevents "PageView" vs "pageview" being treated as different types
    df['event_type'] = df['event_type'].str.lower()
    
    # ============================================================
    # STEP 6: JSON SERIALIZATION
    # ============================================================
    # PostgreSQL JSONB columns require JSON strings, not Python dicts
    # Ensure properties column exists (create if missing)
    if 'properties' not in df.columns:
        df['properties'] = None
    
    def serialize_properties(x):
        """
        Convert properties to JSON string format.
        
        Handles multiple input types:
        - None/NaN -> empty JSON object {}
        - Python dict -> JSON string
        - Already a JSON string -> validate and return as-is
        - Invalid data -> empty JSON object {}
        """
        if pd.isna(x) or x is None:
            return json.dumps({})  # Empty JSON object
        elif isinstance(x, dict):
            return json.dumps(x)  # Convert dict to JSON string
        elif isinstance(x, str):
            # If already a string, validate it's valid JSON
            try:
                json.loads(x)  # Validate it's valid JSON
                return x  # Return as-is if valid
            except (json.JSONDecodeError, TypeError):
                # Invalid JSON string, return empty object
                return json.dumps({})
        else:
            # Unexpected type, return empty object
            return json.dumps({})
    
    # Apply serialization to each row's properties column
    df['properties'] = df['properties'].apply(serialize_properties)
    
    logger.info(f"Cleaned to {len(df)} user events")
    return df


def clean_subscriptions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and transform subscription data.
    
    Args:
        df: Raw subscriptions DataFrame
    
    Returns:
        Cleaned DataFrame
    """
    logger.info(f"Cleaning {len(df)} subscriptions")
    
    if df.empty:
        return df
    
    required_cols = ['subscription_id', 'user_id', 'plan_name', 'status', 'start_date']
    validate_dataframe(df, required_cols)
    
    # Convert dates to datetime
    date_columns = ['start_date', 'end_date', 'created_at', 'updated_at']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Ensure updated_at is never null (use created_at or current timestamp as fallback)
    if 'updated_at' in df.columns:
        df['updated_at'] = df['updated_at'].fillna(df.get('created_at', pd.Timestamp.now()))
        df['updated_at'] = df['updated_at'].fillna(pd.Timestamp.now())
        # Remove any rows that still have null updated_at (shouldn't happen, but safety check)
        initial_count = len(df)
        df = df[df['updated_at'].notna()]
        if len(df) < initial_count:
            logger.warning(f"Removed {initial_count - len(df)} subscriptions with invalid updated_at")
    
    # Validate status values
    valid_statuses = ['active', 'cancelled', 'expired', 'trial']
    df = df[df['status'].isin(valid_statuses)]
    
    # Ensure monthly_revenue is numeric and non-negative
    if 'monthly_revenue' in df.columns:
        df['monthly_revenue'] = pd.to_numeric(df['monthly_revenue'], errors='coerce')
        df['monthly_revenue'] = df['monthly_revenue'].fillna(0)
        df = df[df['monthly_revenue'] >= 0]
    
    # Remove duplicates
    initial_count = len(df)
    df = df.drop_duplicates(subset=['subscription_id'], keep='last')
    if len(df) < initial_count:
        logger.warning(f"Removed {initial_count - len(df)} duplicate subscriptions")
    
    logger.info(f"Cleaned to {len(df)} subscriptions")
    return df


def clean_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and transform transaction data.
    
    Args:
        df: Raw transactions DataFrame
    
    Returns:
        Cleaned DataFrame
    """
    logger.info(f"Cleaning {len(df)} transactions")
    
    if df.empty:
        return df
    
    required_cols = ['transaction_id', 'user_id', 'amount', 'transaction_type', 'status']
    validate_dataframe(df, required_cols)
    
    # Convert transaction_date to datetime
    if 'transaction_date' in df.columns:
        df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce')
        df = df[df['transaction_date'].notna()]
    
    # Ensure amount is numeric
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
    df = df[df['amount'].notna()]
    
    # Validate transaction types
    valid_types = ['payment', 'refund', 'upgrade', 'downgrade']
    df = df[df['transaction_type'].isin(valid_types)]
    
    # Validate status
    valid_statuses = ['completed', 'pending', 'failed']
    df = df[df['status'].isin(valid_statuses)]
    
    # Remove duplicates
    initial_count = len(df)
    df = df.drop_duplicates(subset=['transaction_id'], keep='first')
    if len(df) < initial_count:
        logger.warning(f"Removed {initial_count - len(df)} duplicate transactions")
    
    logger.info(f"Cleaned to {len(df)} transactions")
    return df


def clean_user_profiles(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and transform user profile data.
    
    Args:
        df: Raw user profiles DataFrame
    
    Returns:
        Cleaned DataFrame
    """
    logger.info(f"Cleaning {len(df)} user profiles")
    
    if df.empty:
        return df
    
    required_cols = ['user_id', 'email', 'created_at']
    validate_dataframe(df, required_cols)
    
    # Convert created_at to datetime
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    df = df[df['created_at'].notna()]
    
    # Validate email format (basic check)
    df = df[df['email'].str.contains('@', na=False)]
    
    # Remove duplicates, keeping most recent
    initial_count = len(df)
    df = df.drop_duplicates(subset=['user_id'], keep='last')
    if len(df) < initial_count:
        logger.warning(f"Removed {initial_count - len(df)} duplicate profiles")
    
    logger.info(f"Cleaned to {len(df)} user profiles")
    return df


def enrich_user_events(df: pd.DataFrame, user_profiles: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """
    Enrich user events with user profile data.
    
    Args:
        df: User events DataFrame
        user_profiles: Optional user profiles DataFrame for enrichment
    
    Returns:
        Enriched DataFrame with enrichment columns (country, signup_source, company_size)
    """
    if user_profiles is None or user_profiles.empty or df.empty:
        # Ensure enrichment columns exist even if no enrichment data
        enrichment_cols = ['country', 'signup_source', 'company_size']
        for col in enrichment_cols:
            if col not in df.columns:
                df[col] = None
        return df
    
    logger.info("Enriching user events with profile data")
    
    # Merge user profile data
    enrichment_cols = ['country', 'signup_source', 'company_size']
    available_cols = [col for col in enrichment_cols if col in user_profiles.columns]
    
    if available_cols:
        # Remove duplicates from user_profiles to avoid creating duplicate events
        user_profiles_unique = user_profiles.drop_duplicates(subset=['user_id'], keep='last')
        if len(user_profiles_unique) < len(user_profiles):
            logger.warning(f"Removed {len(user_profiles) - len(user_profiles_unique)} duplicate user_ids from profiles before enrichment")
        
        df = df.merge(
            user_profiles_unique[['user_id'] + available_cols],
            on='user_id',
            how='left'
        )
        logger.info(f"Enriched with columns: {available_cols}")
    else:
        logger.warning("No enrichment columns available in user_profiles")
    
    # Ensure all enrichment columns exist (fill missing ones with None)
    for col in enrichment_cols:
        if col not in df.columns:
            df[col] = None
    
    return df
