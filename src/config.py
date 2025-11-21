import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
    DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
    
    # Paths
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    DATA_DIR = os.path.join(BASE_DIR, "data")
    SAMPLE_LOGS_DIR = os.path.join(DATA_DIR, "sample_logs")
    SAMPLE_SQL_DIR = os.path.join(DATA_DIR, "sample_sql")
    SAMPLE_NOTEBOOKS_DIR = os.path.join(DATA_DIR, "sample_notebooks")
    
    @classmethod
    def is_connected(cls):
        return bool(cls.DATABRICKS_HOST and cls.DATABRICKS_TOKEN)
