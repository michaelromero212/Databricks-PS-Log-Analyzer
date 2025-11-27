from fastapi import FastAPI, HTTPException, BackgroundTasks, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from typing import List
import json
import asyncio
import os

from .schemas import JobRun, AnalysisRequest, AnalysisReport, NotebookWriteRequest
from .databricks_client import DatabricksClient
from .inference import engine

app = FastAPI(title="Databricks Log Analyzer API")

# Templates
templates_dir = os.path.join(os.path.dirname(__file__), "..", "templates")
templates = Jinja2Templates(directory=templates_dir)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

client = DatabricksClient()

@app.on_event("startup")
async def startup_event():
    # Load models on startup (default to local, can be configured)
    # In a real app, might want to do this lazily or via background task
    engine.load_models(mode="local")

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    """
    Serve the operations dashboard.
    """
    return templates.TemplateResponse("dashboard.html", {"request": request})

@app.get("/api/logs/recent", response_model=List[JobRun])
async def get_recent_logs():
    """
    Fetch recent Databricks job run logs.
    """
    return client.fetch_recent_job_logs()

@app.post("/api/analyze")
async def analyze_logs(request: AnalysisRequest):
    """
    Trigger analysis (blocking). For UI, prefer /api/stream_analyze.
    """
    # This is a synchronous wrapper around the generator for non-streaming clients
    report = None
    for event in engine.analyze_logs_stream(request.run_id, request.logs):
        if event.startswith("data: "):
            data = json.loads(event[6:])
            if data.get("status") == "complete":
                report = data.get("report")
    
    if not report:
        raise HTTPException(status_code=500, detail="Analysis failed")
    return report

@app.get("/api/stream_analyze")
async def stream_analyze(run_id: str):
    """
    SSE Endpoint. In a real POST scenario, we'd pass logs in body, 
    but EventSource standard only supports GET. 
    Workaround: Pass ID and fetch logs internally, or use fetch+ReadableStream in frontend.
    For this demo, we'll fetch logs by ID internally.
    """
    # 1. Fetch logs for the run_id
    runs = client.fetch_recent_job_logs()
    target_run = next((r for r in runs if r.run_id == run_id), None)
    
    if not target_run:
        # Stream an error event
        async def error_gen():
            yield f"data: {json.dumps({'error': 'Run not found'})}\n\n"
        return StreamingResponse(error_gen(), media_type="text/event-stream")

    return StreamingResponse(
        engine.analyze_logs_stream(run_id, target_run.logs), 
        media_type="text/event-stream"
    )

@app.post("/api/recommendation/to-notebook")
async def write_to_notebook(request: NotebookWriteRequest):
    success = client.write_notebook_cell(request.notebook_path, request.content)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to write to notebook")
    return {"status": "success"}
