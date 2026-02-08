/*
 * Dimension: Date
 * 
 * Business Purpose:
 * Standard date dimension for time-based analysis.
 * Pre-populated with all dates to enable easy joins and time intelligence.
 *
 * Grain: One row per calendar day
 * 
 * Design Decision:
 * - Pre-populated for 5 years (2023-2028) to cover historical and future data
 * - Includes fiscal calendar attributes (assuming fiscal year = calendar year)
 */

CREATE TABLE IF NOT EXISTS dim_date (
    -- Primary Key (integer format: YYYYMMDD)
    date_key INT PRIMARY KEY,
    
    -- Full Date
    full_date DATE NOT NULL UNIQUE,
    
    -- Calendar Attributes
    day_of_week INT NOT NULL,         -- 1=Monday, 7=Sunday
    day_of_week_name VARCHAR(10) NOT NULL,
    day_of_month INT NOT NULL,
    day_of_year INT NOT NULL,
    
    -- Week
    week_of_year INT NOT NULL,
    week_start_date DATE NOT NULL,
    week_end_date DATE NOT NULL,
    
    -- Month
    month_number INT NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    month_start_date DATE NOT NULL,
    month_end_date DATE NOT NULL,
    
    -- Quarter
    quarter_number INT NOT NULL,
    quarter_name VARCHAR(2) NOT NULL,  -- 'Q1', 'Q2', 'Q3', 'Q4'
    
    -- Year
    year_number INT NOT NULL,
    
    -- Fiscal Calendar (assuming fiscal = calendar)
    fiscal_year INT NOT NULL,
    fiscal_quarter INT NOT NULL,
    
    -- Flags
    is_weekend BOOLEAN NOT NULL,
    is_month_end BOOLEAN NOT NULL,
    is_quarter_end BOOLEAN NOT NULL,
    is_year_end BOOLEAN NOT NULL
);

-- Generate dates from 2023 to 2028
INSERT INTO dim_date
SELECT 
    TO_CHAR(d, 'YYYYMMDD')::INT as date_key,
    d as full_date,
    EXTRACT(ISODOW FROM d)::INT as day_of_week,
    TO_CHAR(d, 'Day') as day_of_week_name,
    EXTRACT(DAY FROM d)::INT as day_of_month,
    EXTRACT(DOY FROM d)::INT as day_of_year,
    EXTRACT(WEEK FROM d)::INT as week_of_year,
    DATE_TRUNC('week', d)::DATE as week_start_date,
    (DATE_TRUNC('week', d) + INTERVAL '6 days')::DATE as week_end_date,
    EXTRACT(MONTH FROM d)::INT as month_number,
    TO_CHAR(d, 'Month') as month_name,
    DATE_TRUNC('month', d)::DATE as month_start_date,
    (DATE_TRUNC('month', d) + INTERVAL '1 month - 1 day')::DATE as month_end_date,
    EXTRACT(QUARTER FROM d)::INT as quarter_number,
    'Q' || EXTRACT(QUARTER FROM d)::TEXT as quarter_name,
    EXTRACT(YEAR FROM d)::INT as year_number,
    EXTRACT(YEAR FROM d)::INT as fiscal_year,
    EXTRACT(QUARTER FROM d)::INT as fiscal_quarter,
    EXTRACT(ISODOW FROM d) IN (6, 7) as is_weekend,
    d = (DATE_TRUNC('month', d) + INTERVAL '1 month - 1 day')::DATE as is_month_end,
    d = (DATE_TRUNC('quarter', d) + INTERVAL '3 months - 1 day')::DATE as is_quarter_end,
    EXTRACT(MONTH FROM d) = 12 AND EXTRACT(DAY FROM d) = 31 as is_year_end
FROM GENERATE_SERIES('2023-01-01'::DATE, '2028-12-31'::DATE, '1 day'::INTERVAL) d
ON CONFLICT (date_key) DO NOTHING;

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_dim_date_full_date ON dim_date(full_date);
CREATE INDEX IF NOT EXISTS idx_dim_date_year_month ON dim_date(year_number, month_number);

/*
 * Example Business Questions This Enables:
 * - What's MRR trend by month/quarter?
 * - How does churn vary by day of week?
 * - Compare this quarter vs same quarter last year
 */
