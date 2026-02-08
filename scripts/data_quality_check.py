"""
Data quality check script.

Validates data warehouse integrity using the DataQualityChecker framework.
Performs comprehensive checks including record counts, null values, 
duplicates, referential integrity, and data freshness.
"""

import sys
from pathlib import Path

# Add src to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
from src.utils.database import create_db_engine
from src.utils.logger import setup_logger
from src.data_quality import DataQualityChecker

# Load environment variables from project root
load_dotenv(dotenv_path=project_root / '.env')

logger = setup_logger(name="data_quality")


def check_data_quality():
    """
    Perform comprehensive data quality checks on warehouse tables.
    
    Uses the DataQualityChecker framework to validate:
    - Record counts (non-zero for critical tables)
    - Null values in required fields
    - Duplicate primary keys
    - Referential integrity between tables
    - Valid status values
    - Data freshness
    
    Raises exception if checks fail (for Airflow task failure handling).
    """
    logger.info("=" * 60)
    logger.info("Starting Data Quality Checks")
    logger.info("=" * 60)
    
    try:
        engine = create_db_engine()
        
        # Build the quality check suite using fluent interface
        checker = DataQualityChecker(engine, "Warehouse Quality Suite")
        
        # Configure all expectations
        result = (checker
            # Row count checks - ensure data was loaded
            .expect_row_count("user_events", min_count=1)
            .expect_row_count("user_profiles", min_count=1)
            .expect_row_count("subscriptions", min_count=1)
            .expect_row_count("transactions", min_count=1)
            
            # Null checks - critical fields must have values
            .expect_no_nulls("user_events", ["event_id", "user_id", "timestamp"])
            .expect_no_nulls("user_profiles", ["user_id", "email"])
            .expect_no_nulls("subscriptions", ["subscription_id", "user_id"])
            .expect_no_nulls("transactions", ["transaction_id", "subscription_id"])
            
            # Uniqueness checks - primary keys must be unique
            .expect_unique("user_events", ["event_id"])
            .expect_unique("user_profiles", ["user_id"])
            .expect_unique("subscriptions", ["subscription_id"])
            .expect_unique("transactions", ["transaction_id"])
            
            # Referential integrity - foreign keys must exist
            .expect_referential_integrity(
                "subscriptions", "user_id",
                "user_profiles", "user_id"
            )
            .expect_referential_integrity(
                "transactions", "subscription_id",
                "subscriptions", "subscription_id"
            )
            .expect_referential_integrity(
                "user_events", "user_id",
                "user_profiles", "user_id"
            )
            
            # Valid values - statuses must be from allowed set
            .expect_values_in_set(
                "subscriptions", "status",
                ["active", "cancelled", "expired", "trial"]
            )
            
            # Freshness - data should be recent
            .expect_freshness("user_events", "ingested_at", max_age_hours=48)
            
            # Run all checks
            .run()
        )
        
        # Print the summary report
        print(result.summary())
        
        # Log individual results
        for check_result in result.results:
            if check_result.passed:
                logger.info(str(check_result))
            elif check_result.severity == "warning":
                logger.warning(str(check_result))
            else:
                logger.error(str(check_result))
        
        # Raise if any critical checks failed
        if not result.success:
            raise ValueError(
                f"Data quality checks failed: {result.failed_checks} error(s), "
                f"{result.warning_checks} warning(s)"
            )
        
        logger.info("All data quality checks passed!")
        return result
            
    except Exception as e:
        logger.error(f"Data quality check failed: {e}")
        raise


if __name__ == '__main__':
    try:
        check_data_quality()
        sys.exit(0)
    except Exception as e:
        logger.error(f"Data quality check failed: {e}")
        sys.exit(1)
