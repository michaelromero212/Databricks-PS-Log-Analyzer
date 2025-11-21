import re

class NotebookOptimizer:
    def analyze_notebook(self, content):
        findings = []
        
        # Rule: Repeated reads without cache
        if content.count("spark.read") > 1 and ".cache()" not in content and ".persist()" not in content:
            findings.append({
                "rule_id": "NB_001",
                "description": "Repeated reads without caching",
                "severity": "medium",
                "recommendation": "You are reading data multiple times. Consider using df.cache() or df.persist() if the data fits in memory."
            })
            
        # Rule: Missing broadcast hint on joins (heuristic)
        if ".join(" in content and "broadcast(" not in content:
             findings.append({
                "rule_id": "NB_002",
                "description": "Join without broadcast hint",
                "severity": "low",
                "recommendation": "If one side of the join is small, use F.broadcast(small_df) to avoid shuffles."
            })
            
        # Rule: Inline SQL SELECT *
        if re.search(r'spark\.sql\(".*?SELECT \*.*?"\)', content, re.IGNORECASE):
            findings.append({
                "rule_id": "NB_003",
                "description": "Inline SQL uses SELECT *",
                "severity": "medium",
                "recommendation": "Avoid SELECT * in inline SQL strings for better performance and maintainability."
            })
            
        return findings
