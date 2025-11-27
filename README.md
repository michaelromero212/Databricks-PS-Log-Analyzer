# Databricks Log Analyzer & Auto-Fix Generator

A production-ready dashboard for analyzing Databricks job logs, identifying root causes using AI, and suggesting fixes.

## Architecture

*   **Backend**: FastAPI (Python)
    *   Handles log ingestion (Databricks API stubs).
    *   Runs local AI inference (Hugging Face) with API fallback.
    *   Provides SSE streaming for real-time analysis.
*   **Frontend**: React (Vite) + Tailwind CSS
    *   Modern, responsive UI.
    *   Interactive Log Viewer.
    *   Plotly charts for error distribution.

## Setup

### Prerequisites
*   Python 3.9+
*   Node.js 18+
*   Docker (optional)

### Environment Variables
Create a `.env` file in the root (or backend):
```ini
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=dapi...
HUGGINGFACE_API_KEY=hf_... (Optional, for API fallback)
```

### Quick Start (Local)

1.  **Install**: `make install`
2.  **Run**: `make dev`
3.  **Open**: http://localhost:5173

### Quick Start (Docker)

1.  `docker-compose up --build`
2.  **Open**: http://localhost:3000

## Key Features

*   **AI-Powered Analysis**: Uses `all-MiniLM-L6-v2` for clustering and `flan-t5-small` for summarization.
*   **Streaming UI**: Real-time feedback via Server-Sent Events.
*   **Databricks Integration**: Stubs provided for fetching logs and writing back to notebooks.

## Testing

*   **Backend**: `pytest backend/tests` (TODO: Add tests)
*   **Frontend**: `npm test` (TODO: Add tests)
