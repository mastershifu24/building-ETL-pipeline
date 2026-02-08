"""
Load data into the dimensional model.

Transforms raw data from the ETL pipeline into the star schema
(dim_account, fact_subscription_daily, fact_user_events).
"""

import sys
from pathlib import Path
from datetime import datetime

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(dotenv_path=project_root / '.env', override=True)

import pandas as pd
from sqlalchemy import text
from src.utils.database import create_db_engine


def load_dim_account(engine):
    """
    Load account dimension from user_profiles.
    
    For now, we treat each user as their own "account" since the 
    current data doesn't have explicit account groupings.
    In a real B2B setup, multiple users would belong to one account.
    """
    print("\n[Loading] dim_account...")
    
    with engine.connect() as conn:
        # Create accounts from user_profiles
        # Each user becomes an "account" for demo purposes
        conn.execute(text("""
            INSERT INTO dim_account (
                account_id, company_name, industry, company_size,
                country, status, signup_date
            )
            SELECT DISTINCT
                user_id as account_id,
                'Company ' || LEFT(user_id, 8) as company_name,
                COALESCE(industry, 'Technology') as industry,
                COALESCE(company_size, '11-50') as company_size,
                COALESCE(country, 'Unknown') as country,
                'active' as status,
                created_at as signup_date
            FROM user_profiles
            WHERE user_id IS NOT NULL
            ON CONFLICT (account_id) DO NOTHING
        """))
        
        conn.commit()
        
        # Count loaded
        result = conn.execute(text("SELECT COUNT(*) FROM dim_account"))
        count = result.scalar()
        print(f"  [OK] {count} accounts loaded")


def load_fact_user_events(engine):
    """
    Load user events into fact_user_events with dimension keys.
    """
    print("\n[Loading] fact_user_events...")
    
    with engine.connect() as conn:
        # Insert events with date_key lookup
        conn.execute(text("""
            INSERT INTO fact_user_events (
                event_id, date_key, user_id, event_type, 
                event_timestamp, session_id, properties, country
            )
            SELECT 
                ue.event_id,
                TO_CHAR(ue.timestamp, 'YYYYMMDD')::INT as date_key,
                ue.user_id,
                ue.event_type,
                ue.timestamp as event_timestamp,
                ue.session_id,
                ue.properties::jsonb as properties,
                up.country
            FROM user_events ue
            LEFT JOIN user_profiles up ON ue.user_id = up.user_id
            WHERE EXISTS (
                SELECT 1 FROM dim_date dd 
                WHERE dd.date_key = TO_CHAR(ue.timestamp, 'YYYYMMDD')::INT
            )
            ON CONFLICT (event_id) DO NOTHING
        """))
        conn.commit()
        
        # Count loaded
        result = conn.execute(text("SELECT COUNT(*) FROM fact_user_events"))
        count = result.scalar()
        print(f"  [OK] {count} events loaded")


def load_fact_subscription_daily(engine):
    """
    Create daily subscription snapshots for MRR analysis.
    
    This generates one row per account per day based on subscription state.
    """
    print("\n[Loading] fact_subscription_daily...")
    
    with engine.connect() as conn:
        # Get date range from subscriptions
        result = conn.execute(text("""
            SELECT MIN(start_date)::date, MAX(COALESCE(end_date, CURRENT_DATE))::date
            FROM subscriptions
        """))
        row = result.fetchone()
        
        if not row or not row[0]:
            print("  [SKIP] No subscriptions found")
            return
        
        # For simplicity, create snapshots for the last 30 days only
        # Use DISTINCT ON to get one subscription per account per day
        conn.execute(text("""
            INSERT INTO fact_subscription_daily (
                date_key, account_key, plan_key, subscription_status,
                mrr, arr, is_new_subscription, is_churned
            )
            SELECT DISTINCT ON (d.date_key, da.account_key)
                d.date_key,
                da.account_key,
                COALESCE(dp.plan_key, 1) as plan_key,
                s.status as subscription_status,
                COALESCE(dp.monthly_price, 0) as mrr,
                COALESCE(dp.monthly_price * 12, 0) as arr,
                (s.start_date::date = d.full_date) as is_new_subscription,
                (s.status IN ('cancelled', 'expired')) as is_churned
            FROM dim_date d
            CROSS JOIN subscriptions s
            JOIN dim_account da ON s.user_id = da.account_id
            LEFT JOIN dim_plan dp ON LOWER(s.plan_name) = dp.plan_id
            WHERE d.full_date >= CURRENT_DATE - INTERVAL '30 days'
              AND d.full_date <= CURRENT_DATE
              AND d.full_date >= s.start_date::date
              AND (s.end_date IS NULL OR d.full_date <= s.end_date::date)
            ORDER BY d.date_key, da.account_key, s.start_date DESC
            ON CONFLICT (date_key, account_key) DO UPDATE SET
                subscription_status = EXCLUDED.subscription_status,
                mrr = EXCLUDED.mrr,
                is_churned = EXCLUDED.is_churned
        """))
        conn.commit()
        
        # Count and show MRR
        result = conn.execute(text("""
            SELECT 
                COUNT(*) as snapshots,
                SUM(mrr) as total_mrr
            FROM fact_subscription_daily
            WHERE date_key = TO_CHAR(CURRENT_DATE, 'YYYYMMDD')::INT
        """))
        row = result.fetchone()
        print(f"  [OK] {row[0]} daily snapshots, today's MRR: ${row[1] or 0:,.2f}")


def main():
    """Load all dimensional model tables."""
    print("=" * 60)
    print("Loading Dimensional Model")
    print("=" * 60)
    
    engine = create_db_engine()
    
    try:
        load_dim_account(engine)
        load_fact_user_events(engine)
        load_fact_subscription_daily(engine)
        
        print("\n" + "=" * 60)
        print("[DONE] Dimensional model loaded!")
        print("=" * 60)
        
        # Show summary
        with engine.connect() as conn:
            print("\nTable Summary:")
            for table in ['dim_account', 'dim_plan', 'dim_date', 
                         'fact_user_events', 'fact_subscription_daily']:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                count = result.scalar()
                print(f"  {table}: {count:,} rows")
        
    except Exception as e:
        print(f"\n[ERROR] {e}")
        raise


if __name__ == '__main__':
    main()
