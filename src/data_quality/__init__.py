"""
Data Quality Framework.

Provides reusable, configurable data quality checks 
inspired by Great Expectations patterns.
"""

from .expectations import DataQualityChecker
from .checks import (
    check_row_count,
    check_null_values,
    check_duplicates,
    check_referential_integrity,
    check_value_range,
    check_freshness,
)

__all__ = [
    'DataQualityChecker',
    'check_row_count',
    'check_null_values', 
    'check_duplicates',
    'check_referential_integrity',
    'check_value_range',
    'check_freshness',
]
