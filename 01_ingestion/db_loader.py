import sqlite3
import csv
import os

def load_database(data_dir="data", db_dir="database", schema_path="02_credit_engine/schema.sql"):
    os.makedirs(db_dir, exist_ok=True)
    db_path = os.path.join(db_dir, 'lidra.db')
    
    print(f"Connecting to {db_path}...")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("Executing schema...")
    with open(schema_path, 'r') as f:
        schema = f.read()
    
    # SQLite doesn't support DROP TABLE IF EXISTS CASCADE
    schema = schema.replace(' CASCADE', '')
    cursor.executescript(schema)
    
    # 1. Load Customers
    customers_file = os.path.join(data_dir, 'customers.csv')
    if os.path.exists(customers_file):
        print("Loading customers...")
        with open(customers_file, 'r') as f:
            reader = csv.DictReader(f)
            data = [(r['customer_id'], r['income'], r['credit_score'], r['join_date']) for r in reader]
            cursor.executemany("INSERT INTO customers (customer_id, income, credit_score, join_date) VALUES (?, ?, ?, ?)", data)
            
    # 2. Load Accounts    
    accounts_file = os.path.join(data_dir, 'accounts.csv')
    if os.path.exists(accounts_file):
        print("Loading accounts...")
        with open(accounts_file, 'r') as f:
            reader = csv.DictReader(f)
            data = [(r['account_id'], r['customer_id'], r['status'], r['credit_limit']) for r in reader]
            cursor.executemany("INSERT INTO accounts (account_id, customer_id, status, credit_limit) VALUES (?, ?, ?, ?)", data)
            
    # 3. Load Clean Transactions
    transactions_file = os.path.join(data_dir, 'clean_transactions.csv')
    if os.path.exists(transactions_file):
        print("Loading transactions into DB...")
        with open(transactions_file, 'r') as f:
            reader = csv.DictReader(f)
            data = [(r['transaction_id'], r['account_id'], r['timestamp'], r['amount'], r['currency'], r['merchant_id'], r['transaction_type']) for r in reader]
            # Chunking the insert to save memory
            chunk_size = 10000
            for i in range(0, len(data), chunk_size):
                cursor.executemany("INSERT INTO transactions (transaction_id, account_id, timestamp, amount, currency, merchant_id, transaction_type) VALUES (?, ?, ?, ?, ?, ?, ?)", data[i:i+chunk_size])

    conn.commit()
    print("Database loaded successfully.")
    conn.close()

if __name__ == '__main__':
    load_database()
