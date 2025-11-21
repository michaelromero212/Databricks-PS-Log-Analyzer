import argparse
import os
from databricks_client import DatabricksClient
from config import Config

class SqlRunner:
    def __init__(self):
        self.client = DatabricksClient()

    def run_file(self, filepath):
        if not os.path.exists(filepath):
            print(f"File not found: {filepath}")
            return

        with open(filepath, 'r') as f:
            sql = f.read()

        print(f"--- Executing: {os.path.basename(filepath)} ---")
        
        if self.client.connected:
            # Real execution logic would go here
            # For now, we simulate it
            print("Sending query to Databricks...")
            # result = self.client.execute_sql(sql) 
            print("Query submitted successfully (Simulated).")
        else:
            print("Offline Mode: Cannot execute SQL against Databricks.")
            print("Showing mock execution plan:")
            print(f"PLAN: Scan {os.path.basename(filepath)} -> Filter -> Project")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run SQL file on Databricks")
    parser.add_argument("--file", required=True, help="Path to SQL file")
    args = parser.parse_args()
    
    runner = SqlRunner()
    runner.run_file(args.file)
