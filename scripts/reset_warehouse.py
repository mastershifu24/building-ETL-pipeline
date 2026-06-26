"""
Truncate warehouse tables for a clean CI or dev run.

Used by GitHub Actions before --small data generation so referential
integrity checks pass against a consistent dataset.
"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from sqlalchemy import text

from src.utils.database import create_db_engine

WAREHOUSE_TABLES = (
    "staging_user_events",
    "user_events",
    "transactions",
    "subscriptions",
    "user_profiles",
    "accounts",
)


def main() -> None:
    engine = create_db_engine()
    table_list = ", ".join(WAREHOUSE_TABLES)
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table_list} RESTART IDENTITY CASCADE"))
    print(f"Truncated warehouse tables: {table_list}")


if __name__ == "__main__":
    main()
