/*
 * Dimension: Account (Company)
 * 
 * Business Purpose:
 * Represents B2B customer accounts (companies) that subscribe to our SaaS product.
 * This is the primary entity for revenue attribution and churn analysis.
 *
 * Grain: One row per account
 * 
 * Design Decision:
 * - Using Type 1 SCD (overwrite) for simplicity in this implementation
 * - For production, consider Type 2 SCD to track historical changes
 */

CREATE TABLE IF NOT EXISTS dim_account (
    -- Primary Key
    account_key SERIAL PRIMARY KEY,
    
    -- Natural Key (from source system)
    account_id VARCHAR(255) NOT NULL UNIQUE,
    
    -- Descriptive Attributes
    company_name VARCHAR(255) NOT NULL,
    industry VARCHAR(100),
    company_size VARCHAR(50),  -- '1-10', '11-50', '51-200', '201-1000', '1000+'
    country VARCHAR(100) NOT NULL,
    
    -- Account Status
    status VARCHAR(50) NOT NULL,  -- 'active', 'churned', 'trial'
    
    -- Dates
    signup_date TIMESTAMP NOT NULL,
    first_paid_date TIMESTAMP,  -- NULL if never converted from trial
    churn_date TIMESTAMP,       -- NULL if still active
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_dim_account_status ON dim_account(status);
CREATE INDEX IF NOT EXISTS idx_dim_account_industry ON dim_account(industry);
CREATE INDEX IF NOT EXISTS idx_dim_account_signup_date ON dim_account(signup_date);

/*
 * Example Business Questions This Enables:
 * - How many active accounts by industry?
 * - What's the average time from signup to first payment?
 * - Which company sizes have the highest churn rate?
 */
