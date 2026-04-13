import csv
import os
from datetime import datetime, timedelta

def detect_smurfing(input_dir="data", output_dir="data"):
    input_file = os.path.join(input_dir, 'clean_transactions.csv')
    if not os.path.exists(input_file):
        print(f"Error: {input_file} not found. Run cleaner.py first.")
        return
        
    print(f"Running Smurfing Structuring Detection on {input_file}...")
    
    # 1. Load deposits
    deposits = []
    with open(input_file, 'r', newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['transaction_type'] == 'DEPOSIT':
                amt = float(row['amount'])
                # Look for amounts just below threshold (e.g. 9000 to 9999)
                if 9000 <= amt < 10000:
                    row['parsed_datetime'] = datetime.strptime(row['timestamp'], '%Y-%m-%d %H:%M:%S')
                    row['amount'] = amt
                    deposits.append(row)
                    
    # Sort by account and time
    deposits.sort(key=lambda x: (x['account_id'], x['parsed_datetime']))
    
    # 2. Detect clusters per account
    alerts = []
    
    # Simple sliding window per account
    # We flag if we see > 2 deposits matching criteria within 72 hours
    current_account = None
    window_start = None
    cluster = []
    
    for dep in deposits:
        acc = dep['account_id']
        
        if acc != current_account:
            # new account, evaluate previous cluster if any
            if len(cluster) >= 3:
                alerts.append(cluster)
            current_account = acc
            cluster = [dep]
            window_start = dep['parsed_datetime']
            continue
            
        time_diff = dep['parsed_datetime'] - window_start
        if time_diff <= timedelta(hours=72):
            cluster.append(dep)
        else:
            if len(cluster) >= 3:
                alerts.append(cluster)
            window_start = dep['parsed_datetime']
            cluster = [dep]
            
    # evaluate last cluster
    if len(cluster) >= 3:
        alerts.append(cluster)
        
    # 3. Save Report
    os.makedirs(output_dir, exist_ok=True)
    out_path = os.path.join(output_dir, 'aml_smurfing_alerts.csv')
    
    total_alerts = 0
    with open(out_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['account_id', 'cluster_size', 'total_amount', 'start_time', 'end_time', 'transaction_ids'])
        writer.writeheader()
        
        for cl in alerts:
            total_alerts += 1
            writer.writerow({
                'account_id': cl[0]['account_id'],
                'cluster_size': len(cl),
                'total_amount': sum(tx['amount'] for tx in cl),
                'start_time': cl[0]['timestamp'],
                'end_time': cl[-1]['timestamp'],
                'transaction_ids': "|".join([tx['transaction_id'] for tx in cl])
            })
            
    print(f"AML Smurfing Detector found {total_alerts} structuring events. Log saved to {out_path}")

if __name__ == '__main__':
    detect_smurfing()
