from typing import List, Optional, Dict, Any
from pydantic import BaseModel
from datetime import datetime

class LogEntry(BaseModel):
    timestamp: str
    level: str
    message: str
    stage: Optional[str] = None
    stack_trace: Optional[str] = None

class JobRun(BaseModel):
    run_id: str
    job_name: str
    start_time: str
    status: str
    logs: List[LogEntry]

class AnalysisRequest(BaseModel):
    run_id: str
    logs: List[LogEntry]
    inference_mode: str = "local"  # "local" or "huggingface-api"

class SuggestedFix(BaseModel):
    description: str
    code_snippet: Optional[str] = None
    priority: str  # High, Medium, Low

class TuningParam(BaseModel):
    param: str
    value: str
    confidence: float
    reason: str

class ClusterResult(BaseModel):
    cluster_id: int
    error_pattern: str
    frequency: int
    root_cause: str
    fixes: List[SuggestedFix]
    tuning_params: List[TuningParam]
    severity_score: float

class AnalysisReport(BaseModel):
    run_id: str
    summary: str
    clusters: List[ClusterResult]
    plotly_data: Dict[str, Any]  # Plotly JSON
    notebook_cell_text: str

class NotebookWriteRequest(BaseModel):
    notebook_path: str
    content: str
