import os
import json
import requests
from config import Config

class DatabricksClient:
    def __init__(self):
        self.host = Config.DATABRICKS_HOST
        self.token = Config.DATABRICKS_TOKEN
        self.connected = Config.is_connected()

    def list_runs(self, limit=5):
        """
        Fetches recent job runs. 
        If not connected, returns mock data from sample_logs.
        """
        if self.connected:
            try:
                url = f"{self.host}/api/2.1/jobs/runs/list?limit={limit}"
                headers = {"Authorization": f"Bearer {self.token}"}
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                return response.json().get("runs", [])
            except Exception as e:
                print(f"Connection failed: {e}. Falling back to mock mode.")
                return self._get_mock_runs()
        else:
            return self._get_mock_runs()

    def get_run_log(self, run_id):
        """
        Fetches output for a specific run.
        If not connected, looks for a matching JSON file in sample_logs.
        """
        if self.connected:
            # In a real scenario, we'd fetch the output. 
            # For simplicity in this demo, we might just return run info or implement full log fetching.
            # This is a placeholder for the real API call.
            pass 
        
        return self._get_mock_log(run_id)

    def _get_mock_runs(self):
        """Reads all JSON files in sample_logs to simulate a list of runs."""
        runs = []
        if not os.path.exists(Config.SAMPLE_LOGS_DIR):
            return []
            
        for filename in os.listdir(Config.SAMPLE_LOGS_DIR):
            if filename.endswith(".json"):
                filepath = os.path.join(Config.SAMPLE_LOGS_DIR, filename)
                try:
                    with open(filepath, "r") as f:
                        data = json.load(f)
                        # Construct a minimal run object from the log data
                        runs.append({
                            "run_id": data.get("run_id", 0),
                            "job_id": data.get("job_id", 0),
                            "run_name": data.get("run_name", "Mock Run"),
                            "state": {"result_state": data.get("status", "UNKNOWN")},
                            "start_time": data.get("start_time", 0)
                        })
                except:
                    continue
        return runs

    def _get_mock_log(self, run_id):
        """Finds a specific mock log file by run_id."""
        if not os.path.exists(Config.SAMPLE_LOGS_DIR):
            return None
            
        # Naive search
        for filename in os.listdir(Config.SAMPLE_LOGS_DIR):
            if filename.endswith(".json"):
                filepath = os.path.join(Config.SAMPLE_LOGS_DIR, filename)
                try:
                    with open(filepath, "r") as f:
                        data = json.load(f)
                        if str(data.get("run_id")) == str(run_id):
                            return data
                except:
                    continue
        return None
