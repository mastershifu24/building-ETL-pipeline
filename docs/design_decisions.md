# Design Decisions & Trade-offs

This document explains the key architectural choices in this pipeline and why they were made. Understanding these trade-offs is more valuable than understanding the code itself.

---

## 1. Why PostgreSQL as the Warehouse?

**Decision:** Use PostgreSQL instead of a cloud data warehouse (Snowflake, BigQuery).

**Trade-offs Considered:**

| Option | Pros | Cons |
|--------|------|------|
| PostgreSQL | Free, ACID-compliant, familiar, good for this data volume | Limited parallelism, manual scaling |
| Snowflake | Scales infinitely, columnar storage | Expensive, requires cloud setup |
| BigQuery | Serverless, great for huge data | Expensive, Google lock-in |

**Why PostgreSQL:**
- At this data volume (<1M rows), PostgreSQL performs excellently
- ACID compliance is critical for financial data (subscriptions, transactions)
- No cloud costs during development
- Easy local development with Docker

**When to reconsider:** If daily event volume exceeds 10M rows or query times become unacceptable.

---

## 2. Why Batch Processing Instead of Streaming?

**Decision:** Run the ETL pipeline on a schedule (daily/hourly) rather than real-time streaming.

**Trade-offs Considered:**

| Approach | Latency | Complexity | Cost |
|----------|---------|------------|------|
| Batch (chosen) | Minutes to hours | Low | Low |
| Micro-batch | Seconds to minutes | Medium | Medium |
| Streaming (Kafka) | Milliseconds | High | High |

**Why Batch:**
- Business metrics (MRR, churn) are inherently batch concepts (monthly, weekly)
- Real-time data isn't needed—dashboards refresh hourly
- Simpler error handling and retry logic
- Lower infrastructure cost

**When to reconsider:** If the business needs real-time alerts (e.g., fraud detection) or live dashboards.

---

## 3. Why Dimensional Modeling (Kimball)?

**Decision:** Use star schema with fact and dimension tables instead of a normalized or One Big Table approach.

**Trade-offs Considered:**

| Model | Query Simplicity | Flexibility | Storage |
|-------|------------------|-------------|---------|
| Star Schema (chosen) | High | Medium | Medium |
| Normalized (3NF) | Low | High | Low |
| One Big Table | Very High | Low | High |

**Why Star Schema:**
- Business users understand dimensions (customer, time, product)
- Optimized for aggregate queries (SUM, COUNT, GROUP BY)
- Standard approach—any BI tool works with it
- Clear separation between metrics (facts) and context (dimensions)

**When to reconsider:** If the schema changes frequently, consider Data Vault for flexibility.

---

## 4. Why Daily Subscription Snapshots?

**Decision:** Store one row per account per day in `fact_subscription_daily` instead of just current state.

**Trade-offs Considered:**

| Approach | Storage | Historical Analysis | Complexity |
|----------|---------|---------------------|------------|
| Daily snapshot (chosen) | High | Full history | Medium |
| Current state only | Low | No history | Low |
| Change data capture | Medium | Full history | High |

**Why Daily Snapshots:**
- Enables point-in-time analysis ("What was our MRR on Jan 1?")
- Simple to query—no complex window functions
- Natural fit for batch processing
- Standard practice for SaaS metrics

**When to reconsider:** If storage costs become significant with millions of accounts.

---

## 5. Why Python for Transformations?

**Decision:** Write transformation logic in Python/pandas instead of SQL or dbt.

**Trade-offs Considered:**

| Tool | Learning Curve | Testability | Community |
|------|----------------|-------------|-----------|
| Python (chosen) | Low for devs | High | Large |
| dbt | Medium | Medium | Growing |
| Pure SQL | Low | Low | Large |

**Why Python:**
- Full programming language for complex logic
- Easy to unit test transformations
- Familiar to most data engineers
- Integrates with data science workflows

**When to reconsider:** For a larger team, dbt provides better collaboration and lineage tracking.

---

## 6. Why Great Expectations-Style Data Quality?

**Decision:** Build a custom data quality framework inspired by Great Expectations patterns.

**Why Custom Instead of Great Expectations Directly:**
- Lighter weight—no heavy dependency
- Simpler integration with existing codebase
- Learning exercise in building data quality patterns
- Can migrate to full Great Expectations later

**Key Quality Patterns Implemented:**
- Row count validation (data arrived)
- Null checks (required fields populated)
- Uniqueness (no duplicate keys)
- Referential integrity (foreign keys valid)
- Freshness (data is recent)

---

## 7. Why Docker Compose for Local Development?

**Decision:** Package all services (PostgreSQL, Airflow) in Docker Compose.

**Benefits:**
- One command to start entire environment
- Consistent across developer machines
- Mirrors production patterns
- Easy to tear down and recreate

**Limitations:**
- Doesn't reflect true cloud deployment
- Resource-intensive on local machine

---

## Summary: The 80/20 Principle

These decisions follow the 80/20 rule:

> Get 80% of the value with 20% of the complexity.

A production system at a large company would use Snowflake, Kafka, dbt, and Kubernetes. But for a growth-stage B2B SaaS with a small data team, this stack delivers almost the same business value at a fraction of the cost and complexity.

The goal isn't to build the most sophisticated pipeline—it's to build one that **reliably answers business questions**.
