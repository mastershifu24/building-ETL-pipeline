-- ============================================================
-- SaaS Analytics Data Warehouse - Sample Queries
-- ============================================================
-- These queries demonstrate common analytics use cases
-- Run these in PostgreSQL to explore your data
-- ============================================================

-- Connect to database:
-- docker exec -it saas_postgres psql -U postgres -d saas_analytics

-- ============================================================
-- 1. BASIC COUNTS & OVERVIEW
-- ============================================================

-- Total records per table
SELECT 
    'user_events' as table_name, COUNT(*) as record_count FROM user_events
UNION ALL
SELECT 'subscriptions', COUNT(*) FROM subscriptions
UNION ALL
SELECT 'transactions', COUNT(*) FROM transactions
UNION ALL
SELECT 'user_profiles', COUNT(*) FROM user_profiles;

-- Recent data ingestion
SELECT 
    table_name,
    MAX(ingested_at) as last_ingested,
    COUNT(*) as total_records
FROM (
    SELECT 'user_events' as table_name, ingested_at FROM user_events
    UNION ALL
    SELECT 'subscriptions', ingested_at FROM subscriptions
    UNION ALL
    SELECT 'transactions', ingested_at FROM transactions
    UNION ALL
    SELECT 'user_profiles', ingested_at FROM user_profiles
) combined
GROUP BY table_name
ORDER BY last_ingested DESC;

-- ============================================================
-- 2. USER ANALYTICS
-- ============================================================

-- Active users by country
SELECT 
    country,
    COUNT(DISTINCT user_id) as active_users,
    COUNT(*) as total_events
FROM user_events
WHERE country IS NOT NULL
GROUP BY country
ORDER BY active_users DESC
LIMIT 10;

-- User signup sources breakdown
SELECT 
    signup_source,
    COUNT(*) as user_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
FROM user_profiles
GROUP BY signup_source
ORDER BY user_count DESC;

-- Events by type
SELECT 
    event_type,
    COUNT(*) as event_count,
    COUNT(DISTINCT user_id) as unique_users
FROM user_events
GROUP BY event_type
ORDER BY event_count DESC;

-- ============================================================
-- 3. SUBSCRIPTION ANALYTICS
-- ============================================================

-- Subscription status breakdown
SELECT 
    status,
    COUNT(*) as subscription_count,
    SUM(monthly_revenue) as total_mrr
FROM subscriptions
GROUP BY status
ORDER BY subscription_count DESC;

-- Revenue by plan
SELECT 
    plan_name,
    COUNT(*) as subscription_count,
    SUM(monthly_revenue) as total_mrr,
    AVG(monthly_revenue) as avg_mrr
FROM subscriptions
WHERE status = 'active'
GROUP BY plan_name
ORDER BY total_mrr DESC;

-- Subscription lifecycle (active vs expired)
SELECT 
    CASE 
        WHEN end_date IS NULL THEN 'Active (No End Date)'
        WHEN end_date > CURRENT_TIMESTAMP THEN 'Active'
        ELSE 'Expired'
    END as lifecycle_status,
    COUNT(*) as count,
    SUM(monthly_revenue) as total_revenue
FROM subscriptions
GROUP BY lifecycle_status;

-- ============================================================
-- 4. TRANSACTION ANALYTICS
-- ============================================================

-- Transaction summary by status
SELECT 
    status,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount
FROM transactions
GROUP BY status
ORDER BY transaction_count DESC;

-- Revenue by transaction type
SELECT 
    transaction_type,
    COUNT(*) as count,
    SUM(amount) as total_revenue,
    AVG(amount) as avg_amount
FROM transactions
WHERE status = 'completed'
GROUP BY transaction_type
ORDER BY total_revenue DESC;

-- Monthly revenue trend
SELECT 
    DATE_TRUNC('month', transaction_date) as month,
    COUNT(*) as transaction_count,
    SUM(amount) as monthly_revenue
FROM transactions
WHERE status = 'completed'
GROUP BY month
ORDER BY month DESC
LIMIT 12;

-- ============================================================
-- 5. ENRICHED USER EVENTS ANALYSIS
-- ============================================================

-- Events with user demographics
SELECT 
    ue.event_type,
    ue.country,
    ue.signup_source,
    COUNT(*) as event_count,
    COUNT(DISTINCT ue.user_id) as unique_users
FROM user_events ue
WHERE ue.country IS NOT NULL
GROUP BY ue.event_type, ue.country, ue.signup_source
ORDER BY event_count DESC
LIMIT 20;

-- Feature usage by company size
SELECT 
    company_size,
    COUNT(*) as feature_usage_events,
    COUNT(DISTINCT user_id) as unique_users
FROM user_events
WHERE event_type = 'feature_used' 
  AND company_size IS NOT NULL
GROUP BY company_size
ORDER BY feature_usage_events DESC;

-- ============================================================
-- 6. DATA QUALITY CHECKS
-- ============================================================

-- Check for null required fields
SELECT 
    'user_events' as table_name,
    COUNT(*) FILTER (WHERE event_id IS NULL) as null_event_ids,
    COUNT(*) FILTER (WHERE user_id IS NULL) as null_user_ids,
    COUNT(*) FILTER (WHERE timestamp IS NULL) as null_timestamps
FROM user_events;

-- Check for duplicate event IDs
SELECT 
    event_id,
    COUNT(*) as duplicate_count
FROM user_events
GROUP BY event_id
HAVING COUNT(*) > 1;

-- Check subscription date consistency
SELECT 
    COUNT(*) as invalid_dates
FROM subscriptions
WHERE end_date IS NOT NULL 
  AND end_date < start_date;

-- ============================================================
-- 7. BUSINESS METRICS
-- ============================================================

-- Monthly Recurring Revenue (MRR)
SELECT 
    SUM(monthly_revenue) as total_mrr,
    COUNT(*) as active_subscriptions
FROM subscriptions
WHERE status = 'active';

-- Customer Acquisition by Source
SELECT 
    up.signup_source,
    COUNT(DISTINCT up.user_id) as total_users,
    COUNT(DISTINCT s.subscription_id) as users_with_subscriptions,
    ROUND(100.0 * COUNT(DISTINCT s.subscription_id) / COUNT(DISTINCT up.user_id), 2) as conversion_rate
FROM user_profiles up
LEFT JOIN subscriptions s ON up.user_id = s.user_id
GROUP BY up.signup_source
ORDER BY total_users DESC;

-- User engagement (events per user)
SELECT 
    COUNT(DISTINCT user_id) as total_users,
    COUNT(*) as total_events,
    ROUND(COUNT(*)::numeric / COUNT(DISTINCT user_id), 2) as avg_events_per_user
FROM user_events;
