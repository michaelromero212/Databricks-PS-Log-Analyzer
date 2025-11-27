import os
import json
import time
import numpy as np
import requests
from typing import List, Dict, Any, Generator
from .schemas import LogEntry, AnalysisReport, ClusterResult, SuggestedFix, TuningParam

# Try importing local ML libraries
try:
    from sentence_transformers import SentenceTransformer
    from transformers import pipeline
    LOCAL_ML_AVAILABLE = True
except ImportError:
    LOCAL_ML_AVAILABLE = False

class InferenceEngine:
    def __init__(self):
        self.hf_api_key = os.getenv("HUGGINGFACE_API_KEY")
        self.embed_model = None
        self.gen_pipeline = None
        
        # Models
        self.EMBED_MODEL_ID = "sentence-transformers/all-MiniLM-L6-v2"
        self.GEN_MODEL_ID = "google/flan-t5-small"
        self.HF_API_URL = "https://api-inference.huggingface.co/models"

    def load_models(self, mode: str = "local"):
        """
        Loads models based on mode.
        """
        if mode == "local":
            if not LOCAL_ML_AVAILABLE:
                print("Local ML libraries not installed. Falling back to API.")
                self.mode = "huggingface-api"
                return

            print("Loading local models...")
            try:
                self.embed_model = SentenceTransformer(self.EMBED_MODEL_ID)
                self.gen_pipeline = pipeline("text2text-generation", model=self.GEN_MODEL_ID)
                self.mode = "local"
                print("Local models loaded.")
            except Exception as e:
                print(f"Failed to load local models: {e}. Falling back to API.")
                self.mode = "huggingface-api"
        else:
            self.mode = "huggingface-api"

    def _get_embeddings(self, texts: List[str]) -> np.ndarray:
        if self.mode == "local" and self.embed_model:
            return self.embed_model.encode(texts)
        else:
            # HF Inference API for Feature Extraction
            api_url = f"https://api-inference.huggingface.co/pipeline/feature-extraction/{self.EMBED_MODEL_ID}"
            headers = {"Authorization": f"Bearer {self.hf_api_key}"}
            response = requests.post(api_url, headers=headers, json={"inputs": texts, "options": {"wait_for_model": True}})
            return np.array(response.json())

    def _generate_text(self, prompt: str) -> str:
        if self.mode == "local" and self.gen_pipeline:
            return self.gen_pipeline(prompt, max_length=200)[0]['generated_text']
        else:
            # HF Inference API for Text2Text Generation
            api_url = f"{self.HF_API_URL}/{self.GEN_MODEL_ID}"
            headers = {"Authorization": f"Bearer {self.hf_api_key}"}
            payload = {"inputs": prompt, "parameters": {"max_length": 200}}
            response = requests.post(api_url, headers=headers, json=payload)
            try:
                return response.json()[0]['generated_text']
            except:
                return "Error generating text via API."

    def analyze_logs_stream(self, run_id: str, logs: List[LogEntry]) -> Generator[str, None, None]:
        """
        Generator that yields SSE events.
        """
        # 1. Preprocessing
        yield f"data: {json.dumps({'status': 'processing', 'message': 'Preprocessing logs...'})}\n\n"
        time.sleep(0.5) # Simulate work
        
        error_logs = [log.message for log in logs if log.level in ["ERROR", "FATAL"] or "Exception" in log.message]
        if not error_logs:
            yield f"data: {json.dumps({'status': 'complete', 'message': 'No errors found.'})}\n\n"
            return

        # 2. Embedding & Clustering (Simplified)
        yield f"data: {json.dumps({'status': 'processing', 'message': 'Clustering error patterns...'})}\n\n"
        
        # In a real app, we'd use sklearn.cluster.DBSCAN or similar on embeddings
        # Here we just take unique error messages for simplicity of the demo
        unique_errors = list(set(error_logs))[:3] # Limit to top 3
        
        clusters = []
        
        for i, error_msg in enumerate(unique_errors):
            yield f"data: {json.dumps({'status': 'analyzing', 'message': f'Analyzing cluster {i+1}/{len(unique_errors)}...'})}\n\n"
            
            # 3. Generation
            prompt = f"Explain this Spark error and suggest a fix: {error_msg}"
            root_cause = self._generate_text(prompt)
            
            cluster = ClusterResult(
                cluster_id=i+1,
                error_pattern=error_msg[:100] + "...",
                frequency=error_logs.count(error_msg),
                root_cause=root_cause,
                fixes=[
                    SuggestedFix(description="Check configuration", priority="High"),
                    SuggestedFix(description="Increase memory", priority="Medium")
                ],
                tuning_params=[
                    TuningParam(param="spark.executor.memory", value="8g", confidence=0.85, reason="OOM detected")
                ],
                severity_score=0.9
            )
            clusters.append(cluster)
            
            # Stream partial result
            yield f"data: {json.dumps({'status': 'partial_result', 'cluster': cluster.dict()})}\n\n"

        # 4. Final Report
        report = AnalysisReport(
            run_id=run_id,
            summary=f"Found {len(clusters)} distinct error patterns.",
            clusters=clusters,
            plotly_data={
                "data": [
                    {"x": ["Cluster 1", "Cluster 2"], "y": [c.frequency for c in clusters], "type": "bar", "name": "Error Frequency"}
                ],
                "layout": {"title": "Error Distribution"}
            },
            notebook_cell_text=f"# Analysis for Run {run_id}\n\n" + "\n".join([f"## Issue {c.cluster_id}\n{c.root_cause}" for c in clusters])
        )
        
        yield f"data: {json.dumps({'status': 'complete', 'report': report.dict()})}\n\n"

engine = InferenceEngine()
