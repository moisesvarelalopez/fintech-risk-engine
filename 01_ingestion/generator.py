import csv
import random
import uuid
import os
import argparse
from datetime import datetime, timedelta

def get_random_datetime(start, end):
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = random.randrange(int_delta)
    return start + timedelta(seconds=random_second)

def generate_data(num_txn=100000, output_dir="."):
    num_customers = max(100, num_txn // 100)
    num_accounts = int(num_customers * 1.5)
    
    customers = []
    print(f"Generating {num_customers} customers...")
    for i in range(num_customers):
        customers.append({
            'customer_id': f'CUST-{uuid.uuid4().hex[:8].upper()}',
            'income': random.randint(30000, 250000),
            'credit_score': random.randint(300, 850),
            'join_date': get_random_datetime(datetime(2020, 1, 1), datetime(2023, 1, 1)).strftime('%Y-%m-%d')
        })

    accounts = []
    print(f"Generating {num_accounts} accounts...")
    for i in range(num_accounts):
        cust = random.choice(customers)
        accounts.append({
            'account_id': f'ACCT-{uuid.uuid4().hex[:8].upper()}',
            'customer_id': cust['customer_id'],
            'status': random.choices(['ACTIVE', 'CLOSED', 'FROZEN'], weights=[0.9, 0.08, 0.02])[0],
            'credit_limit': random.randint(1000, 50000)
        })

    transactions = []
    print(f"Generating {num_txn} transactions...")
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 12, 31)
    
    currencies = ['USD', 'EUR', 'MXN']
    merchants = [f'MERCH-{uuid.uuid4().hex[:6].upper()}' for _ in range(500)]
    
    # Smurfing injection targets
    smurf_targets = random.sample(accounts, max(5, num_accounts // 100))
    smurf_transactions = []
    
    for acct in smurf_targets:
        # Inject smurfing behavior: multiple deposits just under $10,000
        smurf_date = get_random_datetime(start_date, end_date - timedelta(days=2))
        for _ in range(random.randint(3, 7)):
            smurf_transactions.append({
                'transaction_id': f'TXN-{uuid.uuid4().hex[:12].upper()}',
                'account_id': acct['account_id'],
                'timestamp': (smurf_date + timedelta(hours=random.randint(1, 24))).strftime('%Y-%m-%d %H:%M:%S'),
                'amount': round(random.uniform(9000.00, 9999.00), 2),
                'currency': 'USD',
                'merchant_id': '',
                'transaction_type': 'DEPOSIT'
            })

    # Normal transactions
    for i in range(num_txn - len(smurf_transactions)):
        acct = random.choice(accounts)
        
        # introduce dirty data
        is_duplicate = random.random() < 0.03
        is_bad_amount = random.random() < 0.01
        is_null_merchant = random.random() < 0.05
        is_null_currency = random.random() < 0.02
        is_future_date = random.random() < 0.005
        is_mismatched_curr = random.random() < 0.01
        
        txn_type = random.choices(['PURCHASE', 'DEPOSIT', 'WITHDRAWAL'], weights=[0.8, 0.1, 0.1])[0]
        
        amount = round(random.uniform(5.0, 5000.0), 2)
        if txn_type == 'PURCHASE' and random.random() < 0.1:
            amount = round(random.uniform(5.0, 50.0), 2) # lots of small purchases
            
        if is_bad_amount:
            amount = random.choice([-50.0, 0.0, 9999999.99])
            
        currency = random.choices(currencies, weights=[0.8, 0.1, 0.1])[0] if not is_null_currency else ''
        if is_mismatched_curr:
            # Maybe bad rate logic later, for now just a weird currency string
            currency = 'USD_BAD'
            
        merchant = random.choice(merchants) if not is_null_merchant and txn_type == 'PURCHASE' else ''
        
        ts = get_random_datetime(start_date, end_date)
        if is_future_date:
            ts = ts + timedelta(days=365*5) # 5 years in future
            
        txn = {
            'transaction_id': f'TXN-{uuid.uuid4().hex[:12].upper()}',
            'account_id': acct['account_id'],
            'timestamp': ts.strftime('%Y-%m-%d %H:%M:%S'),
            'amount': amount,
            'currency': currency,
            'merchant_id': merchant,
            'transaction_type': txn_type
        }
        transactions.append(txn)
        
        if is_duplicate:
            dup_txn = txn.copy()
            # slight perturbation maybe or exact duplicate
            transactions.append(dup_txn)
            
    transactions.extend(smurf_transactions)
    # Shuffle to mix duplicates and smurfs
    random.shuffle(transactions)
    
    # Write to CSV
    os.makedirs(output_dir, exist_ok=True)
    
    print("Writing customers.csv...")
    with open(os.path.join(output_dir, 'customers.csv'), 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['customer_id', 'income', 'credit_score', 'join_date'])
        writer.writeheader()
        writer.writerows(customers)
        
    print("Writing accounts.csv...")
    with open(os.path.join(output_dir, 'accounts.csv'), 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['account_id', 'customer_id', 'status', 'credit_limit'])
        writer.writeheader()
        writer.writerows(accounts)
        
    print("Writing raw_transactions.csv...")
    with open(os.path.join(output_dir, 'raw_transactions.csv'), 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['transaction_id', 'account_id', 'timestamp', 'amount', 'currency', 'merchant_id', 'transaction_type'])
        writer.writeheader()
        writer.writerows(transactions[:num_txn]) # cap exactly at num_txn if expanded by duplicates
        
    print(f"Successfully generated {len(customers)} customers, {len(accounts)} accounts, and {num_txn} transactions.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Lidra Data Generator')
    parser.add_argument('--count', type=int, default=100000, help='Number of transactions to generate')
    parser.add_argument('--outDir', type=str, default='data', help='Output directory for CSVs')
    args = parser.parse_args()
    
    generate_data(args.count, args.outDir)
