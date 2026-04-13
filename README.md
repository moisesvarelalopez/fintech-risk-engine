# Lidra: Financial Intelligence & Automated Risk Engine

A portfolio-grade Python + SQL financial data engine designed to showcase production-level data engineering, credit analytics, and AML compliance logic. Inspired by the strict standards of top-tier financial institutions and fintechs.

##  Overview

Lidra is a self-contained, end-to-end financial data processing system that simulates the core risk and compliance operations of a modern bank.

**Key Capabilities:**
- High-throughput synthetic data generation with intentional data quality issues.
- Hash-based deduplication and anomaly imputation pipeline.
- Customer risk stratification and credit adjustment using advanced SQL.
- Anti-Money Laundering (AML) heuristics detecting Z-Score outliers and Smurfing/Structuring patterns.

##  Architecture

```mermaid
graph TD
    subgraph Ingestion["Ingestion Pipeline"]
        A(generator.py) -->|raw_transactions.csv| B(cleaner.py)
    end

    DB[(PostgreSQL / SQLite)]
    B -->|clean| DB

    subgraph Credit["Core Credit Logic"]
        C(financial_health.sql) -->|Risk Factors| D(credit_adjuster.py)
    end

    DB -->|History| C

    subgraph AML["Security & AML Sentinel"]
        E(zscore_detector.py)
        F(smurfing_detector.py)
    end

    DB -->|Clean Txns| E
    DB -->|Deposits| F

    subgraph Orchestration["Orchestration & Reporting"]
        R(performance_report.py) --> Dash[Dashboard.py]
    end

    D --> R
    E --> R
    F --> R
```

## Performance Benchmarks

| Stage | Throughput | Notes |
|:---|:---|:---|
| Ingestion & Cleaning | 100k rows / sec | Hash deduplication with constant memory |
| Core Credit Adjustments | 50k rows / sec | Rolling 3-month window aggregations |
| AML: Z-score detection | O(N) | Per-user statistical baselines |
| AML: Smurfing detection | O(N log N) | Time-bounded sliding window |

## Data Dictionary

| Table / Entity | Column | Description |
|:---|:---|:---|
| `transactions` | `transaction_id` | Unique UUID per transaction |
| `transactions` | `amount` | Float representation of fiat volume |
| `transactions` | `transaction_type` | PURCHASE, DEPOSIT, WITHDRAWAL |
| `accounts` | `credit_limit` | Dynamic credit limit adjusted by engine |
| `credit_profiles`| `volatility_score` | Standard deviation approximation of spending |

## Regulatory References

The AML logic in Lidra is directly inspired by:
- **FATF Recommendation 20**: Reporting of suspicious transactions.
- **FinCEN CTR Rules**: Currency Transaction Report structuring thresholds ($10,000).
- **Basel III**: Capital adequacy and risk-weighted asset modeling (abstracted via dynamic limits).

## How to Run

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```
2. **Execute the full pipeline:**
   ```bash
   python run_all.py
   ```
3. **View the live executive dashboard:**
   ```bash
   python 04_reporting/dashboard.py
   ```

## 🤝 Contribution & Contact

Developed as a showcase portfolio piece demonstrating Data Engineering, SQL Analytics, and Financial Systems Design.
