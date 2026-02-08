/*
 * Dimension: Subscription Plan
 * 
 * Business Purpose:
 * Defines the pricing tiers and features available to customers.
 * Essential for MRR calculation and plan upgrade/downgrade analysis.
 *
 * Grain: One row per plan
 * 
 * Design Decision:
 * - Static dimension (rarely changes)
 * - Price stored here for reference, but actual revenue comes from fact tables
 */

CREATE TABLE IF NOT EXISTS dim_plan (
    -- Primary Key
    plan_key SERIAL PRIMARY KEY,
    
    -- Natural Key
    plan_id VARCHAR(50) NOT NULL UNIQUE,
    
    -- Plan Attributes
    plan_name VARCHAR(100) NOT NULL,  -- 'Free', 'Basic', 'Pro', 'Enterprise'
    plan_tier INT NOT NULL,           -- 0=Free, 1=Basic, 2=Pro, 3=Enterprise
    
    -- Pricing
    monthly_price DECIMAL(10, 2) NOT NULL,
    annual_price DECIMAL(10, 2),      -- NULL if annual not offered
    
    -- Features (simplified)
    max_users INT,                    -- NULL = unlimited
    max_storage_gb INT,
    has_api_access BOOLEAN DEFAULT FALSE,
    has_priority_support BOOLEAN DEFAULT FALSE,
    
    -- Status
    is_active BOOLEAN DEFAULT TRUE,   -- FALSE if plan is deprecated
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Seed with standard SaaS plans
INSERT INTO dim_plan (plan_id, plan_name, plan_tier, monthly_price, annual_price, max_users, has_api_access, has_priority_support)
VALUES 
    ('free', 'Free', 0, 0.00, NULL, 1, FALSE, FALSE),
    ('basic', 'Basic', 1, 29.00, 290.00, 5, FALSE, FALSE),
    ('pro', 'Pro', 2, 99.00, 990.00, 25, TRUE, FALSE),
    ('enterprise', 'Enterprise', 3, 299.00, 2990.00, NULL, TRUE, TRUE)
ON CONFLICT (plan_id) DO NOTHING;

/*
 * Example Business Questions This Enables:
 * - What's the revenue distribution across plan tiers?
 * - Which features correlate with higher retention?
 * - What's the upgrade path most customers take?
 */
