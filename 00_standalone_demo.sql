-- ==============================================================================
-- Lidra Financial Engine: Standalone SQL Demonstration
-- Description: This script provides a self-contained execution environment. 
-- It initializes the relational schema, seeds targeted edge-case data, and 
-- executes the core analytical CTEs and Window Functions for immediate evaluation.
-- ==============================================================================

-- 1. Environment Initialization
DROP TABLE IF EXISTS risk_tranches CASCADE;
DROP TABLE IF EXISTS credit_profiles CASCADE;
DROP TABLE IF EXISTS transactions CASCADE;
DROP TABLE IF EXISTS accounts CASCADE;
DROP TABLE IF EXISTS customers CASCADE;

-- 2. DDL Schema Definitions
CREATE TABLE customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    income NUMERIC,
    credit_score INTEGER,
    join_date DATE
);

CREATE TABLE accounts (
    account_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) REFERENCES customers(customer_id),
    status VARCHAR(20),
    credit_limit NUMERIC
);

CREATE TABLE transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    account_id VARCHAR(50) REFERENCES accounts(account_id),
    timestamp TIMESTAMP,
    amount NUMERIC,
    currency VARCHAR(10),
    merchant_id VARCHAR(50),
    transaction_type VARCHAR(20)
);

CREATE TABLE risk_tranches (
    id INTEGER PRIMARY KEY,
    tranche_name VARCHAR(20),
    description VARCHAR(255)
);

INSERT INTO risk_tranches (id, tranche_name, description) VALUES 
(1, 'GREEN', 'Low risk. Spending under 80% of income.'),
(2, 'YELLOW', 'Medium risk. Spending 80-100% of income.'),
(3, 'RED', 'High risk. Spending > income for 3+ months.');

-- 3. DML Data Seeding (Controlled Edge Cases)
-- Note: Seeding profiles simulating distinct risk categories (compliant vs. over-leveraged)
INSERT INTO customers (customer_id, income, credit_score, join_date) VALUES 
('CUST-SAFE', 120000, 780, '2021-05-10'),
('CUST-RISKY', 45000, 520, '2022-11-20');

INSERT INTO accounts (account_id, customer_id, status, credit_limit) VALUES 
('ACCT-SAFE-01', 'CUST-SAFE', 'ACTIVE', 30000),
('ACCT-RISKY-01', 'CUST-RISKY', 'ACTIVE', 5000);

-- Insert compliant transaction streams (Sustainable utilization)
INSERT INTO transactions (transaction_id, account_id, timestamp, amount, currency, merchant_id, transaction_type) VALUES 
('TXN-001', 'ACCT-SAFE-01', '2023-01-05 10:00:00', 1200, 'USD', 'MERCH-A', 'PURCHASE'),
('TXN-002', 'ACCT-SAFE-01', '2023-02-15 11:30:00', 950, 'USD', 'MERCH-B', 'PURCHASE'),
('TXN-003', 'ACCT-SAFE-01', '2023-03-20 14:00:00', 1100, 'USD', 'MERCH-A', 'PURCHASE'),
('TXN-004', 'ACCT-SAFE-01', '2023-01-25 09:00:00', 4000, 'USD', 'PAYROLL', 'DEPOSIT');

-- Insert high-risk transaction streams (Over-leveraged utilization)
INSERT INTO transactions (transaction_id, account_id, timestamp, amount, currency, merchant_id, transaction_type) VALUES 
('TXN-005', 'ACCT-RISKY-01', '2023-01-10 10:00:00', 4500, 'USD', 'MERCH-C', 'PURCHASE'),
('TXN-006', 'ACCT-RISKY-01', '2023-02-12 11:30:00', 5200, 'USD', 'MERCH-D', 'PURCHASE'),
('TXN-007', 'ACCT-RISKY-01', '2023-03-18 14:00:00', 6100, 'USD', 'MERCH-E', 'PURCHASE'),
('TXN-008', 'ACCT-RISKY-01', '2023-01-30 09:00:00', 1000, 'USD', 'PAYROLL', 'DEPOSIT');

-- ==============================================================================
-- 4. Core Credit Engine Execution & Evaluation
-- Description: Executes rolling aggregations and variance metrics.
-- Note: STRFTIME is utilized here to ensure native compatibility across standard
-- SQLite configurations without requiring specialized PostgreSQL drivers.
-- ==============================================================================
WITH monthly_spending AS (
    SELECT 
        a.customer_id,
        STRFTIME('%Y-%m', t.timestamp) AS spend_month,
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
        ) AS rolling_3m_avg_spend
    FROM monthly_spending
),
volatility_calc AS (
    SELECT 
        rm.customer_id,
        MAX(rm.rolling_3m_avg_spend) AS avg_monthly_spend,
        AVG(ABS(rm.total_spent - rm.rolling_3m_avg_spend)) AS avg_deviation
    FROM rolling_metrics rm
    GROUP BY rm.customer_id
)

SELECT 
    c.customer_id,
    c.income AS annual_income,
    ROUND(c.income / 12.0, 2) AS monthly_income,
    ROUND(COALESCE(v.avg_monthly_spend, 0), 2) AS avg_monthly_spend,
    ROUND(COALESCE(v.avg_monthly_spend, 0) / (c.income / 12.0), 2) AS utilization_ratio,
    CASE 
        WHEN (COALESCE(v.avg_monthly_spend, 0) / (c.income / 12.0)) < 0.8 THEN 'GREEN (Limit +5%)'
        WHEN (COALESCE(v.avg_monthly_spend, 0) / (c.income / 12.0)) <= 1.0 THEN 'YELLOW (Limit Unchanged)'
        ELSE 'RED (Limit -15%)'
    END AS simulated_action
FROM customers c
LEFT JOIN volatility_calc v ON c.customer_id = v.customer_id;
