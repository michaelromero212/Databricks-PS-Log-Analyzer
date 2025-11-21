class LogParser:
    def parse_run(self, run_data):
        """
        Extracts key metrics and errors from a job run JSON.
        """
        findings = []
        
        # Check run status
        state = run_data.get("state", {}).get("result_state", "UNKNOWN")
        if state != "SUCCESS":
            findings.append({
                "type": "job_failure",
                "severity": "high",
                "message": f"Job failed with status: {state}",
                "details": run_data.get("error", "No error details provided.")
            })

        # Check tasks
        tasks = run_data.get("tasks", [])
        for task in tasks:
            if task.get("status") == "FAILED":
                findings.append({
                    "type": "task_failure",
                    "severity": "high",
                    "task_key": task.get("task_key"),
                    "message": task.get("error_message", "Unknown error"),
                    "sql_snippet": task.get("sql_query")
                })
            
            # Check metrics if available
            metrics = task.get("metrics", {})
            if "spill_to_disk" in metrics:
                findings.append({
                    "type": "performance_warning",
                    "severity": "medium",
                    "task_key": task.get("task_key"),
                    "message": f"Spill to disk detected: {metrics['spill_to_disk']}. This indicates memory pressure.",
                    "recommendation": "Increase cluster memory or optimize join/aggregation logic."
                })

        return findings
