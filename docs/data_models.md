# Data Models

## Overview

This document describes the data models used throughout the pipeline.

## Source Data Models

### User Events
Tracks user interactions and behavior.

| Field | Type | Description |
|-------|------|-------------|
| event_id | string | Unique event identifier |
| user_id | string | User identifier |
| event_type | string | Type of event (page_view, click, etc.) |
| timestamp | datetime | When the event occurred |
| properties | JSON | Event-specific properties |
| session_id | string | Session identifier (optional) |

### Subscriptions
Subscription and plan information.

| Field | Type | Description |
|-------|------|-------------|
| subscription_id | string | Unique subscription identifier |
| user_id | string | User identifier |
| plan_name | string | Plan tier (free, basic, pro, enterprise) |
| status | string | Subscription status |
| start_date | datetime | Subscription start date |
| end_date | datetime | Subscription end date (nullable) |
| monthly_revenue | decimal | Monthly revenue amount |
| created_at | datetime | Record creation timestamp |
| updated_at | datetime | Last update timestamp |

### Transactions
Financial transaction records.

| Field | Type | Description |
|-------|------|-------------|
| transaction_id | string | Unique transaction identifier |
| user_id | string | User identifier |
| subscription_id | string | Associated subscription |
| amount | decimal | Transaction amount |
| currency | string | Currency code (USD, EUR, etc.) |
| transaction_type | string | Type (payment, refund, upgrade, downgrade) |
| status | string | Transaction status |
| transaction_date | datetime | When transaction occurred |
| payment_method | string | Payment method used |

### User Profiles
User demographic and account information.

| Field | Type | Description |
|-------|------|-------------|
| user_id | string | Unique user identifier |
| email | string | User email address |
| created_at | datetime | Account creation date |
| signup_source | string | How user signed up |
| country | string | User country |
| company_size | string | Company size (optional) |
| industry | string | Industry (optional) |

## Warehouse Schema

The data warehouse uses PostgreSQL with the following design principles:

- **Normalized structure**: Separate tables for each entity
- **Indexes**: On foreign keys and frequently queried columns
- **Timestamps**: `ingested_at` column for tracking data freshness
- **JSONB**: For flexible event properties storage

## Data Quality Rules

1. **Completeness**: Required fields must not be null
2. **Uniqueness**: Primary keys must be unique
3. **Validity**: Values must conform to expected formats/ranges
4. **Consistency**: Related records must be consistent (e.g., user_id exists)
