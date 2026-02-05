"""
Data quality check script.

Validates data warehouse integrity, record counts, and data consistency.
Performs comprehensive checks including null values, duplicates, and referential integrity.
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from sqlalchemy import text
from src.utils.database import create_db_engine
from src.utils.logger import setup_logger

# Load environment variables from project root
project_root = Path(__file__).parent.parent
load_dotenv(dotenv_path=project_root / '.env')

logger = setup_logger(name="data_quality")


def check_data_quality():
    """
    Perform comprehensive data quality checks on warehouse tables.
    
    Validates:
    - Record counts (non-zero for critical tables)
    - Null values in required fields
    - Duplicate primary keys
    - Referential integrity
    - Date consistency
    
    Raises exception if checks fail (for Airflow task failure handling).
    """
    logger.info("=" * 60)
    logger.info("Starting Data Quality Checks")
    logger.info("=" * 60)
    
    try:
        engine = create_db_engine()
        issues_found = []
        
        with engine.connect() as conn:
            # ============================================================
            # 1. RECORD COUNT CHECKS
            # ============================================================
            logger.info("\n1. Record Count Validation")
            logger.info("-" * 60)
            
            result = conn.execute(text("SELECT COUNT(*) FROM user_events"))
            event_count = result.scalar()
            logger.info(f"✓ User events: {event_count:,}")
            if event_count == 0:
                issues_found.append("No user events found in warehouse")
            
            result = conn.execute(text("SELECT COUNT(*) FROM subscriptions"))
            sub_count = result.scalar()
            logger.info(f"✓ Subscriptions: {sub_count:,}")
            
            result = conn.execute(text("SELECT COUNT(*) FROM transactions"))
            trans_count = result.scalar()
            logger.info(f"✓ Transactions: {trans_count:,}")
            
            result = conn.execute(text("SELECT COUNT(*) FROM user_profiles"))
            profile_count = result.scalar()
            logger.info(f"✓ User profiles: {profile_count:,}")
            if profile_count == 0:
                issues_found.append("No user profiles found in warehouse")
            
            # ============================================================
            # 2. NULL VALUE CHECKS
            # ============================================================
            logger.info("\n2. Null Value Validation")
            logger.info("-" * 60)
            
            # Check user_events required fields
            result = conn.execute(text("""
                SELECT 
                    COUNT(*) FILTER (WHERE event_id IS NULL) as null_event_ids,
                    COUNT(*) FILTER (WHERE user_id IS NULL) as null_user_ids,
                    COUNT(*) FILTER (WHERE timestamp IS NULL) as null_timestamps
                FROM user_events
            """))
            row = result.fetchone()
            if row[0] > 0 or row[1] > 0 or row[2] > 0:
                issues_found.append(f"user_events has nulls: event_id={row[0]}, user_id={row[1]}, timestamp={row[2]}")
            else:
                logger.info("✓ user_events: No nulls in required fields")
            
            # Check subscriptions required fields
            result = conn.execute(text("""
                SELECT COUNT(*) FILTER (WHERE subscription_id IS NULL OR updated_at IS NULL)
                FROM subscriptions
            """))
            null_count = result.scalar()
            if null_count > 0:
                issues_found.append(f"subscriptions has {null_count} rows with null required fields")
            else:
                logger.info("✓ subscriptions: No nulls in required fields")
            
            # ============================================================
            # 3. DUPLICATE CHECKS
            # ============================================================
            logger.info("\n3. Duplicate Validation")
            logger.info("-" * 60)
            
            result = conn.execute(text("""
                SELECT event_id, COUNT(*) as cnt
                FROM user_events
                GROUP BY event_id
                HAVING COUNT(*) > 1
                LIMIT 5
            """))
            duplicates = result.fetchall()
            if duplicates:
                issues_found.append(f"Found {len(duplicates)} duplicate event_ids")
                for dup in duplicates[:3]:
                    logger.warning(f"  Duplicate event_id: {dup[0]} appears {dup[1]} times")
            else:
                logger.info("✓ user_events: No duplicate event_ids")
            
            # ============================================================
            # 4. REFERENTIAL INTEGRITY CHECKS
            # ============================================================
            logger.info("\n4. Referential Integrity Validation")
            logger.info("-" * 60)
            
            # Check if subscription user_ids exist in user_profiles
            result = conn.execute(text("""
                SELECT COUNT(DISTINCT s.user_id)
                FROM subscriptions s
                LEFT JOIN user_profiles up ON s.user_id = up.user_id
                WHERE up.user_id IS NULL
            """))
            orphaned = result.scalar()
            if orphaned > 0:
                issues_found.append(f"Found {orphaned} subscriptions with user_ids not in user_profiles")
            else:
                logger.info("✓ subscriptions: All user_ids exist in user_profiles")
            
            # Check if transaction subscription_ids exist
            result = conn.execute(text("""
                SELECT COUNT(DISTINCT t.subscription_id)
                FROM transactions t
                LEFT JOIN subscriptions s ON t.subscription_id = s.subscription_id
                WHERE s.subscription_id IS NULL
            """))
            orphaned = result.scalar()
            if orphaned > 0:
                issues_found.append(f"Found {orphaned} transactions with invalid subscription_ids")
            else:
                logger.info("✓ transactions: All subscription_ids exist")
            
            # ============================================================
            # 5. DATE CONSISTENCY CHECKS
            # ============================================================
            logger.info("\n5. Date Consistency Validation")
            logger.info("-" * 60)
            
            result = conn.execute(text("""
                SELECT COUNT(*)
                FROM subscriptions
                WHERE end_date IS NOT NULL AND end_date < start_date
            """))
            invalid_dates = result.scalar()
            if invalid_dates > 0:
                issues_found.append(f"Found {invalid_dates} subscriptions with end_date < start_date")
            else:
                logger.info("✓ subscriptions: All dates are consistent")
            
            # ============================================================
            # 6. DATA FRESHNESS CHECK
            # ============================================================
            logger.info("\n6. Data Freshness Check")
            logger.info("-" * 60)
            
            result = conn.execute(text("""
                SELECT MAX(ingested_at) as latest_ingestion
                FROM user_events
            """))
            latest = result.scalar()
            if latest:
                logger.info(f"✓ Latest data ingestion: {latest}")
            else:
                logger.warning("⚠ No ingestion timestamps found")
            
            # ============================================================
            # SUMMARY
            # ============================================================
            logger.info("\n" + "=" * 60)
            if issues_found:
                logger.error("Data Quality Check FAILED")
                logger.error("Issues found:")
                for issue in issues_found:
                    logger.error(f"  - {issue}")
                raise ValueError(f"Data quality checks failed: {len(issues_found)} issue(s) found")
            else:
                logger.info("✓ All Data Quality Checks PASSED")
                logger.info("=" * 60)
            
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
