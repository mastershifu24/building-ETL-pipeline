/*
 * Fact Table: Daily Subscription Snapshot
 * 
 * Business Purpose:
 * Tracks the state of every subscription at a daily grain.
 * This is the foundation for MRR, churn, and retention analysis.
 *
 * Grain: One row per account, per day
 * 
 * Design Decision:
 * - Daily snapshot allows point-in-time analysis
 * - Stores both current and derived metrics
 * - MRR is calculated at load time for query performance
 */

CREATE TABLE IF NOT EXISTS fact_subscription_daily (
    -- Composite Primary Key
    date_key INT NOT NULL,
    account_key INT NOT NULL,
    
    -- Foreign Keys
    plan_key INT NOT NULL,
    
    -- Subscription State
    subscription_status VARCHAR(50) NOT NULL,  -- 'active', 'trial', 'cancelled', 'expired'
    
    -- Financial Metrics
    mrr DECIMAL(10, 2) NOT NULL DEFAULT 0,     -- Monthly Recurring Revenue
    arr DECIMAL(12, 2) NOT NULL DEFAULT 0,     -- Annual Recurring Revenue (MRR * 12)
    
    -- Usage Metrics (populated from user_events)
    active_users INT DEFAULT 0,
    total_events INT DEFAULT 0,
    
    -- Lifecycle Flags
    is_new_subscription BOOLEAN DEFAULT FALSE,  -- First day of subscription
    is_churned BOOLEAN DEFAULT FALSE,           -- Cancelled/expired this day
    is_expansion BOOLEAN DEFAULT FALSE,         -- Upgraded plan this day
    is_contraction BOOLEAN DEFAULT FALSE,       -- Downgraded plan this day
    
    -- Days Counter
    days_since_signup INT,
    days_on_current_plan INT,
    
    -- Metadata
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    PRIMARY KEY (date_key, account_key),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (account_key) REFERENCES dim_account(account_key),
    FOREIGN KEY (plan_key) REFERENCES dim_plan(plan_key)
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_fact_sub_date ON fact_subscription_daily(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_sub_account ON fact_subscription_daily(account_key);
CREATE INDEX IF NOT EXISTS idx_fact_sub_status ON fact_subscription_daily(subscription_status);
CREATE INDEX IF NOT EXISTS idx_fact_sub_mrr ON fact_subscription_daily(mrr) WHERE mrr > 0;

/*
 * Example Business Questions This Enables:
 * - What's total MRR as of any given date?
 * - How many customers churned in Q4?
 * - What's the average revenue per account by industry?
 * - Which accounts expanded their subscription last month?
 */

-- Sample Query: Calculate MRR by Month
/*
SELECT 
    d.year_number,
    d.month_number,
    SUM(f.mrr) as total_mrr,
    COUNT(DISTINCT f.account_key) as active_accounts
FROM fact_subscription_daily f
JOIN dim_date d ON f.date_key = d.date_key
WHERE d.is_month_end = TRUE
  AND f.subscription_status = 'active'
GROUP BY d.year_number, d.month_number
ORDER BY d.year_number, d.month_number;
*/
