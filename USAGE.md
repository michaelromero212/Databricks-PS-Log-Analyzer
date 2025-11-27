# Usage Guide

## API Usage

### Analyze Logs (Blocking)
```bash
curl -X POST http://localhost:8000/api/analyze \
  -H "Content-Type: application/json" \
  -d '{
    "run_id": "1001",
    "logs": [
      {"timestamp": "2023-10-27T10:00:00", "level": "ERROR", "message": "SparkException: Task failed while writing rows."}
    ]
  }'
```

### Stream Analysis (SSE)
In a browser or client supporting EventSource:
```javascript
const evtSource = new EventSource("http://localhost:8000/api/stream_analyze?run_id=1001");
evtSource.onmessage = function(event) {
  console.log("New event:", JSON.parse(event.data));
}
```

## Running Locally

1. **Install Dependencies**:
   ```bash
   make install
   ```

2. **Start Development Servers**:
   ```bash
   make dev
   ```
   - Backend: http://localhost:8000/docs
   - Frontend: http://localhost:5173

3. **Docker**:
   ```bash
   make docker-up
   ```
   - Frontend: http://localhost:3000
