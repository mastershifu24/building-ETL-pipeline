"""
Unit tests for data transformation functions.

Tests data cleaning, validation, and transformation logic.
"""

import pytest
import pandas as pd
from datetime import datetime
from src.transform.transformers import (
    clean_user_events,
    clean_subscriptions,
    clean_transactions,
    clean_user_profiles
)


def test_clean_user_events():
    """
    Test user events cleaning function.
    
    Verifies that event types are normalized to lowercase and
    required fields are validated.
    """
    df = pd.DataFrame({
        'event_id': ['e1', 'e2', 'e3'],
        'user_id': ['u1', 'u2', 'u3'],
        'event_type': ['page_view', 'CLICK', 'feature_used'],
        'timestamp': ['2024-01-01T10:00:00', '2024-01-01T11:00:00', '2024-01-01T12:00:00']
    })
    
    cleaned = clean_user_events(df)
    
    assert len(cleaned) == 3
    assert cleaned['event_type'].str.islower().all()


def test_clean_subscriptions():
    """
    Test subscription cleaning function.
    
    Verifies that monthly revenue is validated as non-negative
    and required fields are present.
    """
    df = pd.DataFrame({
        'subscription_id': ['s1', 's2'],
        'user_id': ['u1', 'u2'],
        'plan_name': ['basic', 'pro'],
        'status': ['active', 'cancelled'],
        'start_date': ['2024-01-01', '2024-01-01'],
        'monthly_revenue': [29.0, 99.0]
    })
    
    cleaned = clean_subscriptions(df)
    
    assert len(cleaned) == 2
    assert cleaned['monthly_revenue'].min() >= 0
