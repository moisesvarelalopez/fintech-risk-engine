import sqlite3
import csv
import os
from datetime import datetime

def get_connection():
    # Fallback to SQLite
    os.makedirs('database', exist_ok=True)
    conn = sqlite3.connect('database/lidra.db')
    return conn, 'sqlite'

def adjust_credit(output_dir="data"):
    conn, db_type = get_connection()
    cursor = conn.cursor()
    
    # We assume schema.sql and data is already populated in DB by a runner script.
    # We will simulate reading the customers and avg monthly spend
    # For standalone test, let's assume we read from the CSVs if DB tables are empty
    
    # Let's write the query to get monthly spend per customer. 
    # For simplicity if DB isn't fully set up in this python script context, 
    # we'll read customers.csv and transactions.csv! But we should use SQL.
    query = """
    SELECT 
        c.customer_id, 
        c.income, 
        c.credit_score, 
        a.account_id, 
        a.credit_limit,
        COALESCE(SUM(t.amount) / 12.0, 0) as avg_monthly_spend
    FROM customers c
    JOIN accounts a ON c.customer_id = a.customer_id
    LEFT JOIN transactions t ON a.account_id = t.account_id AND t.transaction_type = 'PURCHASE'
    GROUP BY c.customer_id, a.account_id, c.income, c.credit_score, a.credit_limit
    """
    
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
    except Exception as e:
        print(f"Skipping DB execute, run the DB load first: {e}")
        return

    change_log = []
    
    print("Evaluating credit limits...")
    for row in rows:
        cust_id, income, score, acct_id, limit, avg_spend = row
        income = float(income)
        limit = float(limit)
        avg_spend = float(avg_spend)
        
        monthly_income = income / 12.0
        spend_ratio = avg_spend / monthly_income if monthly_income > 0 else 0
        
        new_limit = limit
        justification = ""
        risk_tranche = ""
        
        if spend_ratio < 0.8:
            risk_tranche = "GREEN"
            new_limit = limit * 1.05 # 5% increase
            justification = f"Spend ratio {spend_ratio:.2f} < 0.8. Low risk."
        elif spend_ratio <= 1.0:
            risk_tranche = "YELLOW"
            new_limit = limit
            justification = f"Spend ratio {spend_ratio:.2f} between 0.8 and 1.0. Moderate risk."
        else:
            risk_tranche = "RED"
            new_limit = limit * 0.85 # 15% decrease
            justification = f"Spend ratio {spend_ratio:.2f} > 1.0. High risk."
            
        change_log.append({
            'customer_id': cust_id,
            'account_id': acct_id,
            'old_limit': round(limit, 2),
            'new_limit': round(new_limit, 2),
            'risk_tranche': risk_tranche,
            'justification': justification,
            'adjusted_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

    # Save log
    os.makedirs(output_dir, exist_ok=True)
    out_path = os.path.join(output_dir, "credit_adjustments.csv")
    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=['customer_id', 'account_id', 'old_limit', 'new_limit', 'risk_tranche', 'justification', 'adjusted_at'])
        writer.writeheader()
        writer.writerows(change_log)
        
    print(f"Credit limits adjusted for {len(change_log)} accounts. Log saved to {out_path}")
    conn.close()

if __name__ == '__main__':
    adjust_credit()
