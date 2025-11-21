import pytest
from src.sql_parser import SqlParser
from src.log_parser import LogParser

def test_sql_select_star():
    parser = SqlParser()
    sql = "SELECT * FROM users"
    features = parser.parse_sql(sql)
    assert features['select_star'] == True

def test_sql_no_where():
    parser = SqlParser()
    sql = "SELECT id FROM users"
    features = parser.parse_sql(sql)
    assert features['missing_where'] == True

def test_log_parser_failure():
    parser = LogParser()
    run_data = {
        "state": {"result_state": "FAILED"},
        "error": "Some error"
    }
    findings = parser.parse_run(run_data)
    assert len(findings) == 1
    assert findings[0]['type'] == 'job_failure'
