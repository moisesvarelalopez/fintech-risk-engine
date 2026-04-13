import time
import tracemalloc
import json
import os
import subprocess
import sys

def run_stage(name, script_path, args=None):
    print(f"\n[{name}] Starting...")
    tracemalloc.start()
    start_time = time.time()
    
    cmd = [sys.executable, script_path]
    if args:
        cmd.extend(args)
        
    result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', errors='replace')
    
    end_time = time.time()
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    
    duration = end_time - start_time
    peak_mb = peak / 10**6
    
    if result.returncode != 0:
        print(f"[{name}] FAILED!")
        print(result.stderr)
        sys.exit(1)
    else:
        print(f"[{name}] Completed in {duration:.2f}s, Peak Memory: {peak_mb:.2f}MB")
        # For simplicity, we just dump stdout if there's no failure
        # print(result.stdout)
        
    return {
        'duration': duration,
        'peak_memory_mb': peak_mb,
        'stdout': result.stdout
    }

def main():
    print("=========================================")
    print(" Lidra Financial Engine - Pipeline Runner")
    print("=========================================")
    
    os.makedirs('data', exist_ok=True)
    
    metrics = {'stages': {}}
    
    # Pillar A
    metrics['stages']['Data Generation'] = run_stage("Generator", "01_ingestion/generator.py", ["--count", "100000"])
    metrics['stages']['Data Cleaning'] = run_stage("Cleaner", "01_ingestion/cleaner.py")
    
    # Parse data quality from stdout
    stdout = metrics['stages']['Data Cleaning']['stdout']
    dq = {}
    for line in stdout.split('\n'):
        if 'Total Rows Processed' in line: dq['raw_rows'] = int(line.split(':')[-1].strip())
        if 'Clean Rows Output' in line: dq['clean_rows'] = int(line.split(':')[-1].strip())
        if 'Rows Rejected' in line: dq['rejected'] = int(line.split(':')[1].split('(')[0].strip())
        if 'Rows Corrected' in line: dq['corrected'] = int(line.split(':')[1].split('(')[0].strip())
    metrics['data_quality'] = dq
    
    metrics['stages']['Database Loader'] = run_stage("DB Loader", "01_ingestion/db_loader.py")
    
    # Pillar B
    metrics['stages']['Credit Logic'] = run_stage("Credit Engine", "02_credit_engine/credit_adjuster.py")
    
    # Pillar C
    metrics['stages']['Z-Score Anomaly'] = run_stage("AML Z-Score", "03_aml_sentinel/zscore_detector.py")
    metrics['stages']['Smurfing Anomaly'] = run_stage("AML Smurfing", "03_aml_sentinel/smurfing_detector.py")
    
    # Save metrics
    with open('data/metrics.json', 'w') as f:
        json.dump(metrics, f, indent=4)
        
    print("\nPipeline execution complete! Generating performance report...\n")
    subprocess.run([sys.executable, "04_reporting/performance_report.py"])
    
if __name__ == '__main__':
    main()
