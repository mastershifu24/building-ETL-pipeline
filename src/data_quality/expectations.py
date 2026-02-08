"""
DataQualityChecker - Main orchestrator for data quality checks.

Provides a fluent interface for defining and running data quality suites,
inspired by Great Expectations patterns.
"""

from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
from datetime import datetime
from sqlalchemy.engine import Engine

from .checks import (
    CheckResult,
    check_row_count,
    check_null_values,
    check_duplicates,
    check_referential_integrity,
    check_value_range,
    check_freshness,
)


@dataclass
class SuiteResult:
    """Result of running a complete data quality suite."""
    suite_name: str
    run_timestamp: datetime
    total_checks: int
    passed_checks: int
    failed_checks: int
    warning_checks: int
    results: List[CheckResult]
    
    @property
    def success(self) -> bool:
        """Suite passes if no errors (warnings allowed)."""
        return self.failed_checks == 0
    
    def summary(self) -> str:
        """Generate summary report."""
        lines = [
            "=" * 60,
            f"Data Quality Suite: {self.suite_name}",
            f"Run at: {self.run_timestamp}",
            "=" * 60,
            f"Total: {self.total_checks} | Passed: {self.passed_checks} | "
            f"Failed: {self.failed_checks} | Warnings: {self.warning_checks}",
            "-" * 60,
        ]
        
        for result in self.results:
            status = "PASS" if result.passed else ("WARN" if result.severity == "warning" else "FAIL")
            icon = {"PASS": "[OK]", "WARN": "[!]", "FAIL": "[X]"}[status]
            lines.append(f"{icon} {result.check_name}: {result.table_name}")
            lines.append(f"    {result.message}")
        
        lines.append("=" * 60)
        outcome = "PASSED" if self.success else "FAILED"
        lines.append(f"Suite Result: {outcome}")
        lines.append("=" * 60)
        
        return "\n".join(lines)


class DataQualityChecker:
    """
    Orchestrates data quality checks with a fluent builder pattern.
    
    Example:
        checker = DataQualityChecker(engine, "Production Suite")
        result = (checker
            .expect_row_count("user_events", min_count=1000)
            .expect_no_nulls("user_profiles", ["user_id", "email"])
            .expect_unique("transactions", ["transaction_id"])
            .expect_referential_integrity(
                "subscriptions", "user_id", 
                "user_profiles", "user_id"
            )
            .run())
        
        if not result.success:
            raise ValueError(result.summary())
    """
    
    def __init__(self, engine: Engine, suite_name: str = "Default Suite"):
        """
        Initialize the checker.
        
        Args:
            engine: SQLAlchemy database engine
            suite_name: Name for this quality suite (for reporting)
        """
        self.engine = engine
        self.suite_name = suite_name
        self._checks: List[Dict[str, Any]] = []
    
    def expect_row_count(
        self, 
        table_name: str, 
        min_count: int = 1, 
        max_count: Optional[int] = None,
        severity: str = "error"
    ) -> "DataQualityChecker":
        """Add a row count expectation."""
        self._checks.append({
            "func": check_row_count,
            "kwargs": {
                "engine": self.engine,
                "table_name": table_name,
                "min_count": min_count,
                "max_count": max_count,
                "severity": severity
            }
        })
        return self
    
    def expect_no_nulls(
        self, 
        table_name: str, 
        columns: List[str],
        severity: str = "error"
    ) -> "DataQualityChecker":
        """Add a no-nulls expectation for specified columns."""
        self._checks.append({
            "func": check_null_values,
            "kwargs": {
                "engine": self.engine,
                "table_name": table_name,
                "columns": columns,
                "max_null_pct": 0.0,
                "severity": severity
            }
        })
        return self
    
    def expect_null_rate(
        self, 
        table_name: str, 
        columns: List[str],
        max_pct: float,
        severity: str = "warning"
    ) -> "DataQualityChecker":
        """Add a null rate threshold expectation."""
        self._checks.append({
            "func": check_null_values,
            "kwargs": {
                "engine": self.engine,
                "table_name": table_name,
                "columns": columns,
                "max_null_pct": max_pct,
                "severity": severity
            }
        })
        return self
    
    def expect_unique(
        self, 
        table_name: str, 
        columns: List[str],
        severity: str = "error"
    ) -> "DataQualityChecker":
        """Add a uniqueness expectation for column combination."""
        self._checks.append({
            "func": check_duplicates,
            "kwargs": {
                "engine": self.engine,
                "table_name": table_name,
                "columns": columns,
                "severity": severity
            }
        })
        return self
    
    def expect_referential_integrity(
        self,
        table_name: str,
        column: str,
        reference_table: str,
        reference_column: str,
        severity: str = "error"
    ) -> "DataQualityChecker":
        """Add a referential integrity expectation."""
        self._checks.append({
            "func": check_referential_integrity,
            "kwargs": {
                "engine": self.engine,
                "table_name": table_name,
                "column": column,
                "reference_table": reference_table,
                "reference_column": reference_column,
                "severity": severity
            }
        })
        return self
    
    def expect_values_in_set(
        self,
        table_name: str,
        column: str,
        allowed_values: List[Any],
        severity: str = "error"
    ) -> "DataQualityChecker":
        """Add an allowed values expectation."""
        self._checks.append({
            "func": check_value_range,
            "kwargs": {
                "engine": self.engine,
                "table_name": table_name,
                "column": column,
                "allowed_values": allowed_values,
                "severity": severity
            }
        })
        return self
    
    def expect_value_range(
        self,
        table_name: str,
        column: str,
        min_value: Optional[Any] = None,
        max_value: Optional[Any] = None,
        severity: str = "error"
    ) -> "DataQualityChecker":
        """Add a value range expectation."""
        self._checks.append({
            "func": check_value_range,
            "kwargs": {
                "engine": self.engine,
                "table_name": table_name,
                "column": column,
                "min_value": min_value,
                "max_value": max_value,
                "severity": severity
            }
        })
        return self
    
    def expect_freshness(
        self,
        table_name: str,
        timestamp_column: str,
        max_age_hours: int = 24,
        severity: str = "warning"
    ) -> "DataQualityChecker":
        """Add a data freshness expectation."""
        self._checks.append({
            "func": check_freshness,
            "kwargs": {
                "engine": self.engine,
                "table_name": table_name,
                "timestamp_column": timestamp_column,
                "max_age_hours": max_age_hours,
                "severity": severity
            }
        })
        return self
    
    def run(self) -> SuiteResult:
        """
        Execute all registered checks and return results.
        
        Returns:
            SuiteResult with all check outcomes
        """
        results: List[CheckResult] = []
        
        for check in self._checks:
            try:
                result = check["func"](**check["kwargs"])
                results.append(result)
            except Exception as e:
                # If check fails to execute, record as failed
                results.append(CheckResult(
                    check_name=check["func"].__name__,
                    table_name=check["kwargs"].get("table_name", "unknown"),
                    passed=False,
                    expected="Check to execute",
                    actual=str(e),
                    message=f"Check failed to execute: {e}",
                    severity="error"
                ))
        
        passed = sum(1 for r in results if r.passed)
        failed = sum(1 for r in results if not r.passed and r.severity == "error")
        warnings = sum(1 for r in results if not r.passed and r.severity == "warning")
        
        return SuiteResult(
            suite_name=self.suite_name,
            run_timestamp=datetime.now(),
            total_checks=len(results),
            passed_checks=passed,
            failed_checks=failed,
            warning_checks=warnings,
            results=results
        )
