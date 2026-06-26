"""
SaaS Analytics Dashboard — reads from Neon Postgres.

Local:  streamlit run dashboard/app.py  (uses .env DATABASE_URL)
Cloud:  deploy on Streamlit Community Cloud with DATABASE_URL in Secrets
"""

import os
import sys
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import streamlit as st
from sqlalchemy import text

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

st.set_page_config(
    page_title="SaaS Analytics",
    page_icon="📊",
    layout="wide",
)


def _clean_url(value: str) -> str:
    return value.strip().strip('"').strip("'")


def _resolve_database_url() -> str | None:
    """
    Resolve Neon connection string from Streamlit secrets, env, or local .env.

    Streamlit Cloud secrets must be TOML, e.g.:
        DATABASE_URL = "postgresql://user:pass@host/neondb?sslmode=require"
    """
    # 1. Streamlit secrets (cloud) — check before .env so cloud never hits localhost
    try:
        secrets = st.secrets
        for key in ("DATABASE_URL", "database_url"):
            if key in secrets:
                url = _clean_url(str(secrets[key]))
                if url:
                    return url
        if "database" in secrets and "url" in secrets["database"]:
            url = _clean_url(str(secrets["database"]["url"]))
            if url:
                return url
    except Exception:
        pass

    # 2. Process environment (some hosts inject this directly)
    url = _clean_url(os.environ.get("DATABASE_URL", ""))
    if url:
        return url

    # 3. Local .env for development
    from dotenv import load_dotenv

    load_dotenv(ROOT / ".env")
    url = _clean_url(os.environ.get("DATABASE_URL", ""))
    if url:
        return url

    return None


def _connection_hint(database_url: str | None) -> str:
    if not database_url:
        return "DATABASE_URL not found — using local POSTGRES_* defaults (localhost)"
    host = urlparse(database_url).hostname or "unknown"
    return f"DATABASE_URL set (host: {host})"


@st.cache_resource
def get_engine():
    database_url = _resolve_database_url()
    if not database_url:
        raise RuntimeError(
            "DATABASE_URL is not set. On Streamlit Cloud, open App settings → Secrets "
            'and add: DATABASE_URL = "postgresql://..." then reboot the app.'
        )
    os.environ["DATABASE_URL"] = database_url
    from src.utils.database import create_db_engine

    return create_db_engine()


@st.cache_data(ttl=300)
def query(sql: str) -> pd.DataFrame:
    return pd.read_sql(text(sql), get_engine())


def main() -> None:
    st.title("B2B SaaS Analytics")
    st.caption(
        "Live warehouse metrics from Neon · refreshed by "
        "[GitHub Actions ETL](https://github.com/mastershifu24/building-ETL-pipeline/actions)"
    )

    database_url = _resolve_database_url()
    with st.sidebar:
        st.caption(_connection_hint(database_url))

    try:
        counts = query("""
            SELECT 'user_events' AS table_name, COUNT(*)::bigint AS count FROM user_events
            UNION ALL SELECT 'user_profiles', COUNT(*)::bigint FROM user_profiles
            UNION ALL SELECT 'subscriptions', COUNT(*)::bigint FROM subscriptions
            UNION ALL SELECT 'transactions', COUNT(*)::bigint FROM transactions
        """)
        count_map = dict(zip(counts["table_name"], counts["count"]))
    except Exception as exc:
        st.error(f"Could not connect to the warehouse: {exc}")
        st.markdown(
            """
**Streamlit Cloud fix** (App settings → Secrets). Paste exactly this format —
include the `DATABASE_URL =` line, not just the URL:

```toml
DATABASE_URL = "postgresql://USER:PASSWORD@HOST/neondb?sslmode=require"
```

Then click **Save** → **Reboot app** (or Manage app → Restart).

Use the same Neon pooled URL as your GitHub Actions secret.
            """
        )
        return

    mrr_row = query("""
        SELECT COALESCE(SUM(monthly_revenue), 0) AS total_mrr
        FROM subscriptions
        WHERE status = 'active'
    """)
    total_mrr = float(mrr_row["total_mrr"].iloc[0])

    freshness = query("""
        SELECT MAX(ingested_at) AS last_ingested FROM user_events
    """)
    last_ingested = freshness["last_ingested"].iloc[0]

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("User events", f"{count_map.get('user_events', 0):,}")
    col2.metric("Users", f"{count_map.get('user_profiles', 0):,}")
    col3.metric("Subscriptions", f"{count_map.get('subscriptions', 0):,}")
    col4.metric("Transactions", f"{count_map.get('transactions', 0):,}")
    col5.metric("Active MRR", f"${total_mrr:,.0f}")

    if pd.notna(last_ingested):
        st.caption(f"Last ETL load: {last_ingested}")

    left, right = st.columns(2)

    with left:
        st.subheader("Events by type")
        events_by_type = query("""
            SELECT event_type, COUNT(*)::bigint AS event_count
            FROM user_events
            GROUP BY event_type
            ORDER BY event_count DESC
        """)
        st.bar_chart(events_by_type.set_index("event_type"))

        st.subheader("Active users by country")
        by_country = query("""
            SELECT country, COUNT(DISTINCT user_id)::bigint AS active_users
            FROM user_events
            WHERE country IS NOT NULL
            GROUP BY country
            ORDER BY active_users DESC
            LIMIT 10
        """)
        st.dataframe(by_country, use_container_width=True, hide_index=True)

    with right:
        st.subheader("MRR by plan (active)")
        mrr_by_plan = query("""
            SELECT plan_name, SUM(monthly_revenue)::numeric(12,2) AS mrr
            FROM subscriptions
            WHERE status = 'active'
            GROUP BY plan_name
            ORDER BY mrr DESC
        """)
        st.bar_chart(mrr_by_plan.set_index("plan_name"))

        st.subheader("Subscription status")
        sub_status = query("""
            SELECT status, COUNT(*)::bigint AS count
            FROM subscriptions
            GROUP BY status
            ORDER BY count DESC
        """)
        st.dataframe(sub_status, use_container_width=True, hide_index=True)

    st.subheader("Recent completed revenue")
    revenue = query("""
        SELECT
            DATE_TRUNC('month', transaction_date) AS month,
            SUM(amount)::numeric(12,2) AS revenue
        FROM transactions
        WHERE status = 'completed'
        GROUP BY month
        ORDER BY month DESC
        LIMIT 6
    """)
    if not revenue.empty:
        revenue["month"] = revenue["month"].dt.strftime("%Y-%m")
        st.line_chart(revenue.set_index("month"))


if __name__ == "__main__":
    main()
