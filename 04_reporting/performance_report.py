import json
import os
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

def generate_report(metrics_file='data/metrics.json'):
    console = Console()
    
    if not os.path.exists(metrics_file):
        console.print(f"[red]Error: {metrics_file} not found. Run run_all.py first.[/red]")
        return
        
    with open(metrics_file, 'r') as f:
        metrics = json.load(f)
        
    table = Table(title="Lidra Pipeline Performance Benchmark")
    
    table.add_column("Stage", style="cyan", no_wrap=True)
    table.add_column("Time (sec)", style="magenta")
    table.add_column("Peak Memory (MB)", justify="right", style="green")
    table.add_column("Notes", justify="left")
    
    total_time = 0
    total_memory = 0
    
    for stage, data in metrics['stages'].items():
        table.add_row(
            stage, 
            f"{data['duration']:.2f}", 
            f"{data['peak_memory_mb']:.2f}",
            data.get('notes', '')
        )
        total_time += data['duration']
        total_memory = max(total_memory, data['peak_memory_mb'])

    table.add_row("---", "---", "---", "---")
    table.add_row("[bold]Total / Peak[/bold]", f"[bold]{total_time:.2f}[/bold]", f"[bold]{total_memory:.2f}[/bold]", "")
    
    console.print(Panel(table, title="[bold blue]Performance Execution Report[/bold blue]", expand=False))
    
    # Also data quality breakdown
    dq = metrics.get('data_quality', {})
    if dq:
        dq_table = Table(title="Data Quality Improvement")
        dq_table.add_column("Metric", style="cyan")
        dq_table.add_column("Value", style="yellow")
        
        dq_table.add_row("Raw Transactions", str(dq.get('raw_rows', 0)))
        dq_table.add_row("Clean Transactions", str(dq.get('clean_rows', 0)))
        dq_table.add_row("Rejected", str(dq.get('rejected', 0)))
        dq_table.add_row("Corrected", str(dq.get('corrected', 0)))
        
        if dq.get('raw_rows', 0) > 0:
            imp_pct = (dq.get('rejected', 0) + dq.get('corrected', 0)) / dq.get('raw_rows', 1) * 100
            dq_table.add_row("DQ Intervention Rate", f"{imp_pct:.2f}%")
            
        console.print(dq_table)

if __name__ == '__main__':
    generate_report()
