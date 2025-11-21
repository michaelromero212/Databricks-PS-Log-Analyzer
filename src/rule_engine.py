from log_parser import LogParser
from sql_parser import SqlParser
from sql_rules import SqlRules
from optimize_notebook import NotebookOptimizer
import os

class RuleEngine:
    def __init__(self):
        self.log_parser = LogParser()
        self.sql_parser = SqlParser()
        self.sql_rules = SqlRules()
        self.nb_optimizer = NotebookOptimizer()

    def analyze_run(self, run_data):
        return self.log_parser.parse_run(run_data)

    def analyze_sql_file(self, filepath):
        with open(filepath, 'r') as f:
            content = f.read()
        
        features = self.sql_parser.parse_sql(content)
        recommendations = self.sql_rules.check_rules(features, content)
        
        return {
            "file": os.path.basename(filepath),
            "features": features,
            "recommendations": recommendations
        }

    def analyze_notebook_file(self, filepath):
        with open(filepath, 'r') as f:
            content = f.read()
            
        findings = self.nb_optimizer.analyze_notebook(content)
        return {
            "file": os.path.basename(filepath),
            "findings": findings
        }
