import csv
import statistics
import os

def detect_anomalies(input_dir="data", output_dir="data"):
    input_file = os.path.join(input_dir, 'clean_transactions.csv')
    if not os.path.exists(input_file):
        print(f"Error: {input_file} not found. Run cleaner.py first.")
        return
        
    print(f"Running Z-Score Anomaly Detection on {input_file}...")
    
    # 1. Build per-account history
    account_amounts = {}
    transactions = []
    
    with open(input_file, 'r', newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['transaction_type'] == 'PURCHASE':
                acc = row['account_id']
                amt = float(row['amount'])
                if acc not in account_amounts:
                    account_amounts[acc] = []
                account_amounts[acc].append(amt)
            transactions.append(row)
            
    # 2. Compute mean & stdev per account
    account_stats = {}
    for acc, amounts in account_amounts.items():
        if len(amounts) >= 5: # Need enough data points for viable stats
            mean_amt = statistics.mean(amounts)
            std_amt = statistics.stdev(amounts) if len(amounts) > 1 else 0
            account_stats[acc] = {'mean': mean_amt, 'stdev': std_amt}
            
    # 3. Detect anomalies
    anomalies = []
    for txn in transactions:
        if txn['transaction_type'] == 'PURCHASE':
            acc = txn['account_id']
            if acc in account_stats:
                stats = account_stats[acc]
                if stats['stdev'] > 0:
                    z_score = (float(txn['amount']) - stats['mean']) / stats['stdev']
                else:
                    z_score = 0
                    
                if z_score > 3:
                    severity = "CRITICAL" if z_score > 5 else ("HIGH_RISK" if z_score > 4 else "SUSPICIOUS")
                    anomalies.append({
                        'transaction_id': txn['transaction_id'],
                        'account_id': acc,
                        'amount': txn['amount'],
                        'mean_amount': round(stats['mean'], 2),
                        'z_score': round(z_score, 2),
                        'severity': severity,
                        'timestamp': txn['timestamp']
                    })
                    
    # 4. Save results
    os.makedirs(output_dir, exist_ok=True)
    out_path = os.path.join(output_dir, 'aml_zscore_alerts.csv')
    with open(out_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['transaction_id', 'account_id', 'amount', 'mean_amount', 'z_score', 'severity', 'timestamp'])
        writer.writeheader()
        writer.writerows(anomalies)
        
    print(f"AML Z-Score Detector found {len(anomalies)} anomalous transactions. Log saved to {out_path}")

if __name__ == '__main__':
    detect_anomalies()
