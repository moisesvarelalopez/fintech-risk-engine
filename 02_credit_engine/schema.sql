-- 02_credit_engine/schema.sql

DROP TABLE IF EXISTS risk_tranches CASCADE;
DROP TABLE IF EXISTS credit_profiles CASCADE;
DROP TABLE IF EXISTS transactions CASCADE;
DROP TABLE IF EXISTS accounts CASCADE;
DROP TABLE IF EXISTS customers CASCADE;

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

CREATE TABLE credit_profiles (
    customer_id VARCHAR(50),
    calculated_at TIMESTAMP,
    volatility_score NUMERIC,
    payment_behavior_score NUMERIC,
    risk_tranche_id INTEGER REFERENCES risk_tranches(id),
    suggested_limit NUMERIC,
    PRIMARY KEY (customer_id, calculated_at)
);
