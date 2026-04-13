import csv
import os
import time
import random
import statistics
from datetime import datetime
from rich.console import Console
from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich.live import Live

def load_baselines(data_dir='data'):
    # Generar datos base totalmente aleatorios para simular un volumen masivo y realista
    tranches = {
        'GREEN': random.randint(15000, 25000), 
        'YELLOW': random.randint(3000, 8000), 
        'RED': random.randint(500, 1500)
    }
    
    valid_accounts = [f"ACC-{random.randint(100000, 999999)}" for _ in range(1000)]
    
    account_stats = {}
    for acc in valid_accounts:
        account_stats[acc] = {
            'mean': random.uniform(50.0, 5000.0),
            'stdev': random.uniform(10.0, 800.0)
        }
        
    aml_alerts = []
    # Sembrar algunas alertas previas para que el dashboard no este vacio inicialmente
    for _ in range(8):
        acct = random.choice(valid_accounts)
        z = random.uniform(3.1, 7.5)
        sev = "CRITICAL" if z > 5 else ("HIGH_RISK" if z > 4 else "SUSPICIOUS")
        amt = account_stats[acct]['mean'] + (account_stats[acct]['stdev'] * z)
        aml_alerts.append({
            'account_id': acct,
            'amount': amt,
            'z_score': f"{z:.2f}",
            'severity': sev
        })
    aml_alerts.sort(key=lambda x: float(x['z_score']), reverse=True)
    aml_alerts = aml_alerts[:10]

    return tranches, account_stats, valid_accounts, aml_alerts

def generate_dashboard():
    console = Console()
    
    console.print("[yellow]Loading historical analytics & initializing simulation engine...[/]")
    tranches, account_stats, valid_accounts, top_aml = load_baselines()
    
    if not valid_accounts:
        console.print("[red]Error: Could not generate realistic accounts.[/]")
        return
        
    recent_tx = []
    
    def make_layout():
        layout = Layout(name="root")
        layout.split(
            Layout(name="header", size=3),
            Layout(name="main", ratio=1)
        )
        layout["main"].split_row(
            Layout(name="left", ratio=1),
            Layout(name="right", ratio=1)
        )
        layout["right"].split(
            Layout(name="top_right", ratio=1),
            Layout(name="bottom_right", ratio=1)
        )
        return layout
        
    layout = make_layout()
    layout["header"].update(Panel("[bold white on blue] Lidra Financial Engine - Live Active Sentinel Dashboard [/]", style="blue"))

    with Live(layout, refresh_per_second=6):
        try:
            simulation_ticks = 0
            max_ticks = 400 # Límite de la simulación (aprox. 1 o 2 minutos) para que no sea infinito
            
            while simulation_ticks < max_ticks:
                simulation_ticks += 1
                
                # Fluctuar tranches ligeramente para la animacion y el realismo
                for t in ['GREEN', 'YELLOW', 'RED']:
                    if random.random() < 0.4:
                        tranches[t] += random.randint(-2, 4)
                        if tranches[t] < 0: tranches[t] = 0
                        
                # 1. Generate new simulation transaction
                acct = random.choice(valid_accounts)
                
                # Make 5% of new transactions structural anomalies intentionally!
                is_anomaly = random.random() < 0.05
                base = account_stats[acct]['mean']
                stdev = account_stats[acct]['stdev']
                
                if is_anomaly and stdev > 0:
                    amount = base + stdev * random.uniform(3.5, 6.0) # trigger > 3 z-score anomaly
                else:
                    amount = max(1.0, random.gauss(base, stdev if stdev > 0 else 10))
                    
                cur_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                txn_type = random.choices(['PURCHASE', 'WITHDRAWAL', 'TRANSFER', 'PAYMENT'], weights=[0.7, 0.15, 0.1, 0.05])[0]
                
                tx = {
                    'timestamp': cur_time,
                    'account_id': acct,
                    'transaction_type': txn_type,
                    'amount': amount
                }
                recent_tx.insert(0, tx)
                recent_tx = recent_tx[:25] # Roll feed to keep last 25
                
                # 2. Evaluate Anomaly Live inside the runtime
                z_score = 0
                if txn_type == 'PURCHASE' and stdev > 0:
                    z_score = (amount - base) / stdev
                    if z_score > 3:
                        severity = "CRITICAL" if z_score > 5 else ("HIGH_RISK" if z_score > 4 else "SUSPICIOUS")
                        top_aml.append({
                            'account_id': acct,
                            'amount': amount,
                            'z_score': f"{z_score:.2f}",
                            'severity': severity
                        })
                        # Resort and trim to top 10 worst anomalies
                        top_aml.sort(key=lambda x: float(x['z_score']), reverse=True)
                        top_aml = top_aml[:10]

                # 3. Stream data to Rich UI matrices
                feed_table = Table(title="Live Ingestion & Cleaning Feed [SIMULATION ROUTINE]", expand=True)
                feed_table.add_column("Time", style="dim")
                feed_table.add_column("Account", style="cyan")
                feed_table.add_column("Type", style="green")
                feed_table.add_column("Amount", justify="right")
                
                for rtx in recent_tx:
                    color = "red" if (z_score > 3 and rtx == tx) else "white"
                    feed_table.add_row(
                        rtx['timestamp'],
                        rtx['account_id'],
                        rtx['transaction_type'],
                        f"[{color}]${float(rtx['amount']):.2f}[/{color}]"
                    )
                layout["left"].update(Panel(feed_table, title="Ingestion Pipeline"))

                tranche_table = Table(title="Credit Tranche Distribution", expand=True)
                tranche_table.add_column("Tranche", style="bold")
                tranche_table.add_column("Count", justify="right")
                tranche_table.add_row("[green]GREEN[/green]", str(tranches.get('GREEN', 0)))
                tranche_table.add_row("[yellow]YELLOW[/yellow]", str(tranches.get('YELLOW', 0)))
                tranche_table.add_row("[red]RED[/red]", str(tranches.get('RED', 0)))
                layout["top_right"].update(Panel(tranche_table, title="Core Credit Logic"))

                aml_table = Table(title="Top 10 High-Risk Anomalies (Z-Score)", expand=True)
                aml_table.add_column("Account", style="cyan")
                aml_table.add_column("Amount", style="red")
                aml_table.add_column("Z-Score", style="magenta")
                aml_table.add_column("Severity")
                
                for alert in top_aml:
                    color = "red" if alert['severity'] == "CRITICAL" else "yellow"
                    aml_table.add_row(
                        alert['account_id'],
                        f"${float(alert['amount']):.2f}",
                        alert['z_score'],
                        f"[{color}]{alert['severity']}[/{color}]"
                    )
                layout["bottom_right"].update(Panel(aml_table, title="Security & AML Sentinel [LIVE EVALUATION]"))

                # Randomize tick rate para que el feed se vea más realista y fluido
                time.sleep(random.uniform(0.05, 0.25))
                
        except KeyboardInterrupt:
            pass

if __name__ == '__main__':
    generate_dashboard()
