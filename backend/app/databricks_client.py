import json
import os
from typing import List, Dict, Any
from .schemas import JobRun, LogEntry

class DatabricksClient:
    def __init__(self):
        self.host = os.getenv("DATABRICKS_HOST")
        self.token = os.getenv("DATABRICKS_TOKEN")
        # TODO: User must supply DATABRICKS_HOST and DATABRICKS_TOKEN in .env for real API calls

    def fetch_recent_job_logs(self, since_minutes: int = 60) -> List[JobRun]:
        """
        Fetches recent job logs. 
        If credentials are missing, returns local sample data.
        """
        if not self.host or not self.token:
            print("WARNING: No Databricks credentials found. Returning local sample data.")
            return self._load_sample_data()

        # TODO: Implement real Databricks Jobs API call here
        # headers = {"Authorization": f"Bearer {self.token}"}
        # response = requests.get(f"{self.host}/api/2.1/jobs/runs/list", headers=headers)
        
        return self._load_sample_data()

    def _load_sample_data(self) -> List[JobRun]:
        try:
            # Load the sample JSON we copied
            sample_path = os.path.join(os.path.dirname(__file__), "..", "sample_data", "databricks_logs.json")
            with open(sample_path, "r") as f:
                data = json.load(f)
                
            # Adapt the sample format to our schema if necessary
            # Assuming the sample is a single run for simplicity, wrapping in list
            # In a real scenario, we'd parse the specific JSON structure
            
            # Mocking a JobRun from the raw JSON for demonstration
            logs = []
            if isinstance(data, list):
                raw_logs = data
            else:
                raw_logs = data.get("logs", [])

            for entry in raw_logs:
                logs.append(LogEntry(
                    timestamp=entry.get("timestamp", "2023-01-01T00:00:00"),
                    level=entry.get("level", "INFO"),
                    message=entry.get("message", ""),
                    stage=entry.get("stage"),
                    stack_trace=entry.get("stack_trace")
                ))

            return [
                JobRun(
                    run_id="1001",
                    job_name="Nightly ETL - Sales",
                    start_time="2023-10-27T02:00:00",
                    status="FAILED",
                    logs=logs
                ),
                JobRun(
                    run_id="1002",
                    job_name="ML Training Pipeline",
                    start_time="2023-10-27T03:30:00",
                    status="SUCCESS",
                    logs=[LogEntry(timestamp="2023-10-27T03:30:00", level="INFO", message="Training completed successfully.")]
                )
            ]
        except Exception as e:
            print(f"Error loading sample data: {e}")
            return []

    def write_notebook_cell(self, notebook_path: str, content: str) -> bool:
        """
        Stub for writing a suggestion to a Databricks notebook.
        """
        if not self.host or not self.token:
            print("WARNING: No Databricks credentials. Skipping notebook write.")
            return False

        print(f"Writing to notebook {notebook_path} on {self.host}...")
        # TODO: Implement Workspace API call
        # POST /api/2.0/workspace/import or modify
        return True
