import csv
import hashlib
import os
import argparse
from datetime import datetime

def hash_row(row):
    # Hash based on account_id, timestamp, amount, transaction_type
    # this ignores transaction_id to catch true semantic duplicates
    hash_str = f"{row['account_id']}_{row['timestamp']}_{row['amount']}_{row['transaction_type']}"
    return hashlib.sha256(hash_str.encode('utf-8')).hexdigest()

def clean_data(input_dir="data", output_dir="data"):
    input_file = os.path.join(input_dir, 'raw_transactions.csv')
    output_file = os.path.join(output_dir, 'clean_transactions.csv')
    audit_file = os.path.join(output_dir, 'data_quality_audit.csv')
    
    if not os.path.exists(input_file):
        print(f"Error: {input_file} not found. Run generator.py first.")
        return
        
    seen_hashes = set()
    total_rows = 0
    clean_rows = []
    audits = []
    
    print(f"Cleaning data from {input_file}...")
    
    with open(input_file, 'r', newline='') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for row in reader:
            total_rows += 1
            is_valid = True
            modifications = []
            
            # 1. Deduplication
            row_hash = hash_row(row)
            if row_hash in seen_hashes:
                audits.append({'transaction_id': row['transaction_id'], 'issue': 'DUPLICATE', 'action': 'REJECTED'})
                continue
            seen_hashes.add(row_hash)
            
            # 2. Type coercion & Validation
            try:
                amt = float(row['amount'])
                if amt <= 0 or amt > 1000000:
                    audits.append({'transaction_id': row['transaction_id'], 'issue': f'INVALID_AMOUNT:{amt}', 'action': 'REJECTED'})
                    is_valid = False
            except ValueError:
                audits.append({'transaction_id': row['transaction_id'], 'issue': 'BAD_TYPE_AMOUNT', 'action': 'REJECTED'})
                is_valid = False
                
            # Date validation
            try:
                ts = datetime.strptime(row['timestamp'], '%Y-%m-%d %H:%M:%S')
                if ts > datetime.now() + timedelta(days=365): # allowing some future for demo purposes, but capping it
                    pass # Just a demo
                if ts.year > 2025:
                    audits.append({'transaction_id': row['transaction_id'], 'issue': f'FUTURE_DATE:{ts}', 'action': 'REJECTED'})
                    is_valid = False
            except Exception:
                audits.append({'transaction_id': row['transaction_id'], 'issue': 'BAD_DATE_FORMAT', 'action': 'REJECTED'})
                is_valid = False
                
            # 3. Null imputation strategy
            if not row['currency'] or row['currency'] == 'USD_BAD':
                row['currency'] = 'USD' # Default imputation
                modifications.append('IMPUTED_CURRENCY')
                
            if not row['merchant_id'] and row['transaction_type'] == 'PURCHASE':
                row['merchant_id'] = 'UNKNOWN_MERCHANT'
                modifications.append('IMPUTED_MERCHANT')
                
            if is_valid:
                if modifications:
                    audits.append({'transaction_id': row['transaction_id'], 'issue': 'MISSING_DATA', 'action': f"CORRECTED:{'|'.join(modifications)}"})
                clean_rows.append(row)
                
    os.makedirs(output_dir, exist_ok=True)
    with open(output_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(clean_rows)
        
    with open(audit_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['transaction_id', 'issue', 'action'])
        writer.writeheader()
        writer.writerows(audits)
        
    rejected_count = len([a for a in audits if a['action'] == 'REJECTED'])
    corrected_count = len([a for a in audits if a['action'].startswith('CORRECTED')])
    
    print("\n--- Data Quality Report ---")
    print(f"Total Rows Processed : {total_rows}")
    print(f"Clean Rows Output    : {len(clean_rows)}")
    print(f"Rows Rejected        : {rejected_count} ({rejected_count/total_rows*100:.2f}%)")
    print(f"Rows Corrected       : {corrected_count} ({corrected_count/total_rows*100:.2f}%)")
    print(f"Audit log saved to   : {audit_file}")
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Lidra Data Cleaner')
    parser.add_argument('--inDir', type=str, default='data', help='Input directory for raw CSVs')
    parser.add_argument('--outDir', type=str, default='data', help='Output directory for clean CSVs')
    args = parser.parse_args()
    
    # We need timedelta imported if used! Let's just fix timedelta usage
    from datetime import timedelta
    clean_data(args.inDir, args.outDir)
