/*
 * Fact Table: User Events (Transaction Grain)
 * 
 * Business Purpose:
 * Captures individual user interactions with the product.
 * Essential for engagement scoring, feature adoption, and churn prediction.
 *
 * Grain: One row per event
 * 
 * Design Decision:
 * - Transaction grain (not snapshot) because events are immutable
 * - Denormalized with account/user info for query performance
 * - Properties stored as JSONB for flexibility
 */

CREATE TABLE IF NOT EXISTS fact_user_events (
    -- Primary Key
    event_key SERIAL PRIMARY KEY,
    
    -- Natural Key (from source)
    event_id VARCHAR(255) NOT NULL UNIQUE,
    
    -- Foreign Keys
    date_key INT NOT NULL,
    account_key INT,  -- NULL if user not yet linked to account
    
    -- Event Details
    user_id VARCHAR(255) NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,
    
    -- Session Context
    session_id VARCHAR(255),
    
    -- Event Properties (flexible schema)
    properties JSONB,
    
    -- Denormalized for Performance
    country VARCHAR(100),
    plan_name VARCHAR(50),
    
    -- Derived Metrics
    is_activation_event BOOLEAN DEFAULT FALSE,  -- Key milestone events
    is_engagement_event BOOLEAN DEFAULT FALSE,  -- Feature usage events
    
    -- Metadata
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_fact_events_date ON fact_user_events(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_events_user ON fact_user_events(user_id);
CREATE INDEX IF NOT EXISTS idx_fact_events_type ON fact_user_events(event_type);
CREATE INDEX IF NOT EXISTS idx_fact_events_timestamp ON fact_user_events(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_fact_events_account ON fact_user_events(account_key);

/*
 * Example Business Questions This Enables:
 * - Which features are most used by retained customers?
 * - What's the average session duration by plan tier?
 * - How many days until first "activation" event?
 * - Which event patterns predict churn?
 */

-- Sample Query: Daily Active Users by Event Type
/*
SELECT 
    d.full_date,
    f.event_type,
    COUNT(DISTINCT f.user_id) as unique_users,
    COUNT(*) as total_events
FROM fact_user_events f
JOIN dim_date d ON f.date_key = d.date_key
WHERE d.full_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY d.full_date, f.event_type
ORDER BY d.full_date, total_events DESC;
*/
