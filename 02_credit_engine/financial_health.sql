-- 02_credit_engine/financial_health.sql

/*
Financial Health Scorecard & Volatility Engine
Using Advanced CTEs and Window Functions
Note: Uses PostgreSQL specific stddev_pop if available, else a fallback calculation should be used.
For this query, we will use standard aggregates and Window Functions.
*/

WITH monthly_spending AS (
    SELECT 
        a.customer_id,
        DATE_TRUNC('month', t.timestamp) AS spend_month,
        SUM(t.amount) AS total_spent
    FROM transactions t
    JOIN accounts a ON t.account_id = a.account_id
    WHERE t.transaction_type = 'PURCHASE'
    GROUP BY a.customer_id, spend_month
),
rolling_metrics AS (
    SELECT 
        customer_id,
        spend_month,
        total_spent,
        AVG(total_spent) OVER (
            PARTITION BY customer_id 
            ORDER BY spend_month 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS rolling_3m_avg_spend,
        LAG(total_spent, 1) OVER (PARTITION BY customer_id ORDER BY spend_month) AS prev_month_spend,
        LEAD(total_spent, 1) OVER (PARTITION BY customer_id ORDER BY spend_month) AS next_month_spend
    FROM monthly_spending
),
volatility_calc AS (
    SELECT 
        rm.customer_id,
        MAX(rm.rolling_3m_avg_spend) AS avg_monthly_spend,
        -- Simple standard deviation approximation: root mean square of differences
        -- PostgreSQL supports STDDEV() over window, but for best compatibility we use simple variance approximation in outer queries
        AVG(ABS(rm.total_spent - rm.rolling_3m_avg_spend)) AS avg_deviation
    FROM rolling_metrics rm
    GROUP BY rm.customer_id
),
payment_behavior AS (
    SELECT 
        a.customer_id,
        COUNT(CASE WHEN t.transaction_type = 'DEPOSIT' THEN 1 END) AS total_payments,
        -- In our synthetic data, any payment > 0 is considered good.
        -- We will mock "on-time" by looking at frequency of payments.
        COALESCE(COUNT(CASE WHEN t.transaction_type = 'DEPOSIT' THEN 1 END) * 1.0 / NULLIF(COUNT(t.transaction_id), 0), 0) AS payment_ratio
    FROM transactions t
    JOIN accounts a ON t.account_id = a.account_id
    GROUP BY a.customer_id
)

SELECT 
    c.customer_id,
    c.income,
    c.credit_score,
    COALESCE(v.avg_monthly_spend, 0) AS avg_monthly_spend,
    COALESCE(v.avg_deviation, 0) AS volatility_score,
    p.total_payments,
    p.payment_ratio AS payment_behavior_score,
    PERCENT_RANK() OVER (ORDER BY COALESCE(v.avg_monthly_spend, 0) DESC) AS spending_percentile,
    NTILE(4) OVER (ORDER BY c.credit_score ASC) AS credit_quartile
FROM customers c
LEFT JOIN volatility_calc v ON c.customer_id = v.customer_id
LEFT JOIN payment_behavior p ON c.customer_id = p.customer_id;
