import re

class SqlParser:
    def parse_sql(self, sql_content):
        """
        Static analysis of SQL content.
        Returns a dictionary of detected features.
        """
        sql_upper = sql_content.upper()
        
        features = {
            "select_star": bool(re.search(r"SELECT\s+\*", sql_upper)),
            "cross_join": bool(re.search(r"CROSS\s+JOIN", sql_upper)) or bool(re.search(r"FROM\s+\w+\s*,\s*\w+", sql_upper)),
            "missing_where": "WHERE" not in sql_upper,
            "join_detected": "JOIN" in sql_upper,
            "tables": self._extract_tables(sql_content)
        }
        return features

    def _extract_tables(self, sql):
        # Very basic regex for table extraction - can be improved with sqlparse lib
        # Matches FROM table or JOIN table
        tables = re.findall(r"(?:FROM|JOIN)\s+([a-zA-Z0-9_.]+)", sql, re.IGNORECASE)
        return list(set(tables))
