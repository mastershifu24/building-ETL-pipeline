"""
Individual data quality check functions.

Each function returns a CheckResult with success status and details.
These are the building blocks for the DataQualityChecker.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, List, Any
from sqlalchemy import text
from sqlalchemy.engine import Engine


@dataclass
class CheckResult:
    """Result of a single data quality check."""
    check_name: str
    table_name: str
    passed: bool
    expected: Any
    actual: Any
    message: str
    severity: str = "error"  # "error", "warning", "info"
    
    def __str__(self):
        status = "PASS" if self.passed else "FAIL"
        return f"[{status}] {self.check_name} on {self.table_name}: {self.message}"


def check_row_count(
    engine: Engine,
    table_name: str,
    min_count: int = 1,
    max_count: Optional[int] = None,
    severity: str = "error"
) -> CheckResult:
    """
    Check that table has expected number of rows.
    
    Args:
        engine: Database connection
        table_name: Table to check
        min_count: Minimum expected rows (default 1)
        max_count: Maximum expected rows (optional)
        severity: "error", "warning", or "info"
    
    Returns:
        CheckResult with pass/fail status
    """
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
        count = result.scalar()
    
    passed = count >= min_count
    if max_count is not None:
        passed = passed and count <= max_count
    
    expected = f">= {min_count}" + (f" and <= {max_count}" if max_count else "")
    message = f"Found {count:,} rows (expected {expected})"
    
    return CheckResult(
        check_name="row_count",
        table_name=table_name,
        passed=passed,
        expected=expected,
        actual=count,
        message=message,
        severity=severity
    )


def check_null_values(
    engine: Engine,
    table_name: str,
    columns: List[str],
    max_null_pct: float = 0.0,
    severity: str = "error"
) -> CheckResult:
    """
    Check that specified columns have acceptable null rate.
    
    Args:
        engine: Database connection
        table_name: Table to check
        columns: List of column names to check
        max_null_pct: Maximum acceptable null percentage (0.0 = no nulls allowed)
        severity: "error", "warning", or "info"
    
    Returns:
        CheckResult with pass/fail status
    """
    null_checks = " + ".join([f"CASE WHEN {col} IS NULL THEN 1 ELSE 0 END" for col in columns])
    
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT 
                COUNT(*) as total,
                SUM({null_checks}) as null_count
            FROM {table_name}
        """))
        row = result.fetchone()
        total = row[0]
        null_count = row[1] or 0
    
    if total == 0:
        return CheckResult(
            check_name="null_values",
            table_name=table_name,
            passed=True,
            expected=f"<= {max_null_pct}%",
            actual="0%",
            message="Table is empty, no nulls possible",
            severity="info"
        )
    
    null_pct = (null_count / (total * len(columns))) * 100
    passed = null_pct <= max_null_pct
    
    return CheckResult(
        check_name="null_values",
        table_name=table_name,
        passed=passed,
        expected=f"<= {max_null_pct}%",
        actual=f"{null_pct:.2f}%",
        message=f"Columns {columns}: {null_pct:.2f}% null (expected <= {max_null_pct}%)",
        severity=severity
    )


def check_duplicates(
    engine: Engine,
    table_name: str,
    columns: List[str],
    severity: str = "error"
) -> CheckResult:
    """
    Check for duplicate values in specified columns.
    
    Args:
        engine: Database connection
        table_name: Table to check
        columns: Columns that should be unique together
        severity: "error", "warning", or "info"
    
    Returns:
        CheckResult with pass/fail status
    """
    cols_str = ", ".join(columns)
    
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT {cols_str}, COUNT(*) as cnt
            FROM {table_name}
            GROUP BY {cols_str}
            HAVING COUNT(*) > 1
            LIMIT 10
        """))
        duplicates = result.fetchall()
    
    passed = len(duplicates) == 0
    
    return CheckResult(
        check_name="no_duplicates",
        table_name=table_name,
        passed=passed,
        expected="0 duplicates",
        actual=f"{len(duplicates)} duplicate groups found",
        message=f"Columns {columns}: {'No duplicates' if passed else f'{len(duplicates)}+ duplicate groups'}",
        severity=severity
    )


def check_referential_integrity(
    engine: Engine,
    table_name: str,
    column: str,
    reference_table: str,
    reference_column: str,
    severity: str = "error"
) -> CheckResult:
    """
    Check that all values in column exist in reference table.
    
    Args:
        engine: Database connection
        table_name: Table to check
        column: Column with foreign key values
        reference_table: Table that should contain all values
        reference_column: Column in reference table
        severity: "error", "warning", or "info"
    
    Returns:
        CheckResult with pass/fail status
    """
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT COUNT(DISTINCT t.{column})
            FROM {table_name} t
            LEFT JOIN {reference_table} r ON t.{column} = r.{reference_column}
            WHERE r.{reference_column} IS NULL
              AND t.{column} IS NOT NULL
        """))
        orphan_count = result.scalar()
    
    passed = orphan_count == 0
    
    return CheckResult(
        check_name="referential_integrity",
        table_name=table_name,
        passed=passed,
        expected="0 orphaned records",
        actual=f"{orphan_count} orphaned values",
        message=f"{table_name}.{column} -> {reference_table}.{reference_column}: {orphan_count} orphaned",
        severity=severity
    )


def check_value_range(
    engine: Engine,
    table_name: str,
    column: str,
    min_value: Optional[Any] = None,
    max_value: Optional[Any] = None,
    allowed_values: Optional[List[Any]] = None,
    severity: str = "error"
) -> CheckResult:
    """
    Check that column values are within expected range or set.
    
    Args:
        engine: Database connection
        table_name: Table to check
        column: Column to validate
        min_value: Minimum allowed value
        max_value: Maximum allowed value
        allowed_values: List of allowed discrete values
        severity: "error", "warning", or "info"
    
    Returns:
        CheckResult with pass/fail status
    """
    with engine.connect() as conn:
        if allowed_values:
            # Check against allowed set
            values_str = ", ".join([f"'{v}'" for v in allowed_values])
            result = conn.execute(text(f"""
                SELECT COUNT(*), COUNT(*) FILTER (WHERE {column} NOT IN ({values_str}))
                FROM {table_name}
                WHERE {column} IS NOT NULL
            """))
            row = result.fetchone()
            total, violations = row[0], row[1]
            
            passed = violations == 0
            expected = f"values in {allowed_values}"
            actual = f"{violations} values outside allowed set"
            
        else:
            # Check against range
            conditions = []
            if min_value is not None:
                conditions.append(f"{column} < {min_value}")
            if max_value is not None:
                conditions.append(f"{column} > {max_value}")
            
            if not conditions:
                return CheckResult(
                    check_name="value_range",
                    table_name=table_name,
                    passed=True,
                    expected="No constraints specified",
                    actual="N/A",
                    message="No range constraints provided",
                    severity="info"
                )
            
            where_clause = " OR ".join(conditions)
            result = conn.execute(text(f"""
                SELECT COUNT(*)
                FROM {table_name}
                WHERE {where_clause}
            """))
            violations = result.scalar()
            
            passed = violations == 0
            expected = f"{min_value} <= {column} <= {max_value}"
            actual = f"{violations} values out of range"
    
    return CheckResult(
        check_name="value_range",
        table_name=table_name,
        passed=passed,
        expected=expected,
        actual=actual,
        message=f"{column}: {actual}" if not passed else f"{column}: All values in range",
        severity=severity
    )


def check_freshness(
    engine: Engine,
    table_name: str,
    timestamp_column: str,
    max_age_hours: int = 24,
    severity: str = "warning"
) -> CheckResult:
    """
    Check that table has been updated recently.
    
    Args:
        engine: Database connection
        table_name: Table to check
        timestamp_column: Column containing timestamps
        max_age_hours: Maximum hours since last update
        severity: "error", "warning", or "info"
    
    Returns:
        CheckResult with pass/fail status
    """
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT MAX({timestamp_column})
            FROM {table_name}
        """))
        latest = result.scalar()
    
    if latest is None:
        return CheckResult(
            check_name="freshness",
            table_name=table_name,
            passed=False,
            expected=f"Data within {max_age_hours} hours",
            actual="No data found",
            message="No timestamps found in table",
            severity=severity
        )
    
    age = datetime.now() - latest
    age_hours = age.total_seconds() / 3600
    passed = age_hours <= max_age_hours
    
    return CheckResult(
        check_name="freshness",
        table_name=table_name,
        passed=passed,
        expected=f"<= {max_age_hours} hours old",
        actual=f"{age_hours:.1f} hours old",
        message=f"Latest record: {latest} ({age_hours:.1f}h ago)",
        severity=severity
    )
