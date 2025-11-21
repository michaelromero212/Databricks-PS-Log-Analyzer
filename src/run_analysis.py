import argparse
import os
import json
from config import Config
from databricks_client import DatabricksClient
from rule_engine import RuleEngine
from ai_analysis import AIAnalyzer

def main():
    parser = argparse.ArgumentParser(description="Databricks PS Log Analyzer")
    parser.add_argument("--latest", action="store_true", help="Analyze latest job runs")
    parser.add_argument("--job", type=int, help="Analyze specific job ID")
    parser.add_argument("--local-sample", action="store_true", help="Analyze local sample data")
    
    args = parser.parse_args()
    
    client = DatabricksClient()
    engine = RuleEngine()
    ai = AIAnalyzer()
    
    print("=== Databricks PS Log Analyzer ===")
    
    if args.local_sample or (not args.latest and not args.job):
        print("\nRunning in Local Sample Mode...")
        
        # Analyze Logs
        print("\n--- Analyzing Sample Logs ---")
        runs = client.list_runs()
        for run in runs:
            print(f"Run ID: {run['run_id']} | Status: {run['state']['result_state']}")
            # In a real app, we'd fetch the full log details here
            # For this demo, we'll just re-read the file if it matches a sample
            log_data = client.get_run_log(run['run_id'])
            if log_data:
                findings = engine.analyze_run(log_data)
                for f in findings:
                    print(f"  [{f['severity'].upper()}] {f['message']}")
                    if f.get('type') == 'job_failure':
                        print(f"  AI Insight: {ai.analyze_error(f['details'])}")

        # Analyze SQL
        print("\n--- Analyzing Sample SQL ---")
        for f in os.listdir(Config.SAMPLE_SQL_DIR):
            if f.endswith(".sql"):
                path = os.path.join(Config.SAMPLE_SQL_DIR, f)
                res = engine.analyze_sql_file(path)
                if res['recommendations']:
                    print(f"File: {f}")
                    for r in res['recommendations']:
                        print(f"  - {r['description']} ({r['severity']})")

        # Analyze Notebooks
        print("\n--- Analyzing Sample Notebooks ---")
        for f in os.listdir(Config.SAMPLE_NOTEBOOKS_DIR):
            if f.endswith(".py"):
                path = os.path.join(Config.SAMPLE_NOTEBOOKS_DIR, f)
                res = engine.analyze_notebook_file(path)
                if res['findings']:
                    print(f"File: {f}")
                    for r in res['findings']:
                        print(f"  - {r['description']}")

    elif args.latest:
        print("Fetching latest runs from Databricks...")
        runs = client.list_runs()
        print(f"Found {len(runs)} runs.")
        # Implementation for processing real runs would go here

if __name__ == "__main__":
    main()
