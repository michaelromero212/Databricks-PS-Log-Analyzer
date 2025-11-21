class SqlRules:
    def check_rules(self, features, sql_content):
        recommendations = []
        
        if features.get("select_star"):
            recommendations.append({
                "rule_id": "SQL_001",
                "description": "SELECT * detected",
                "severity": "medium",
                "recommendation": "Replace SELECT * with explicit column names to reduce data transfer and improve schema evolution safety."
            })
            
        if features.get("cross_join"):
            recommendations.append({
                "rule_id": "SQL_002",
                "description": "Cartesian Product / Cross Join detected",
                "severity": "high",
                "recommendation": "Ensure this is intentional. Cross joins can explode data volume. Check for missing join conditions."
            })
            
        if features.get("join_detected") and features.get("missing_where"):
            # Heuristic: Joins without filters on large tables are risky
            recommendations.append({
                "rule_id": "SQL_003",
                "description": "Join without WHERE clause",
                "severity": "low",
                "recommendation": "Verify if full table processing is required. Adding filters can significantly reduce shuffle."
            })
            
        # Check for Z-Order/Optimize missing (heuristic based on keywords)
        if "ZORDER" not in sql_content.upper() and "OPTIMIZE" not in sql_content.upper():
             # This is a weak check, usually applies to DDL/Maintenance, but we'll include it as a 'tip' for large queries
             pass

        return recommendations
