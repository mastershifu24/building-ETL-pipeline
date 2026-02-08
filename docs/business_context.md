# Business Context

## The Company

This platform is designed for a B2B SaaS company selling team collaboration software to small and mid-sized businesses.

**Business Model:**
- Monthly and annual subscription plans (Free, Basic, Pro, Enterprise)
- Revenue driven by seat-based pricing within accounts
- Growth through trial conversions and account expansion

## Core Business Metrics

### Monthly Recurring Revenue (MRR)
The total predictable revenue normalized to a monthly amount.

**Why it matters:** MRR is the primary health indicator for a subscription business. Investors, leadership, and sales teams all track MRR growth as the key success metric.

**How we calculate it:**
```sql
SELECT SUM(monthly_price) as mrr
FROM subscriptions s
JOIN dim_plan p ON s.plan_id = p.plan_id
WHERE s.status = 'active'
```

### Churn Rate
The percentage of accounts that cancel their subscription in a given period.

**Why it matters:** High churn indicates product or fit problems. A 5% monthly churn means losing half your customers yearly.

**How we calculate it:**
```sql
-- Monthly churn rate
SELECT 
    COUNT(CASE WHEN status = 'churned' THEN 1 END)::float / 
    COUNT(*) as churn_rate
FROM accounts
WHERE signup_date < date_trunc('month', current_date)
```

### Trial-to-Paid Conversion
The percentage of trial accounts that become paying customers.

**Why it matters:** Low conversion indicates friction in onboarding or unclear value proposition.

### Net Revenue Retention (NRR)
Revenue from existing customers including expansion minus churn.

**Why it matters:** NRR > 100% means you grow even without new customers—the gold standard for B2B SaaS.

## Key Business Questions This Platform Answers

1. **What's our current MRR and how is it trending?**
   - Tracked in `fact_subscription_daily` with daily snapshots

2. **Which customer segments have the highest churn?**
   - Analyze by `dim_account.industry`, `company_size`, or `plan_tier`

3. **What product behaviors predict retention?**
   - Correlate `fact_user_events` patterns with churn outcomes

4. **How long does it take trials to convert?**
   - Measure `days_since_signup` when `is_new_subscription` becomes true

5. **Which features drive expansion revenue?**
   - Track feature usage before plan upgrades

## Data Sources and Their Business Purpose

| Table | Business Purpose |
|-------|------------------|
| `user_events` | Understand product engagement and feature adoption |
| `user_profiles` | Know who our users are and segment them |
| `subscriptions` | Track revenue state and plan changes |
| `transactions` | Financial audit trail and payment health |
| `accounts` | B2B entity for revenue attribution |

## Success Criteria

This data platform succeeds if it enables:

1. **Weekly MRR reporting** in under 5 minutes (not hours)
2. **Churn prediction** 30 days before it happens
3. **Self-service analytics** for product and sales teams
4. **Data trust** - stakeholders believe the numbers
