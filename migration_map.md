# Migration Map: Streamlit to FastAPI + React

This document maps the original Streamlit components to their new counterparts in the React/FastAPI architecture.

| Feature | Streamlit Implementation | New React/FastAPI Implementation |
| :--- | :--- | :--- |
| **App Layout** | `st.set_page_config`, `st.sidebar` | `frontend/src/App.jsx` (Layout shell), `frontend/src/pages/Dashboard.jsx` |
| **Log Display** | `st.dataframe` or `st.text` | `frontend/src/components/LogViewer.jsx` (Custom Table with Tailwind) |
| **Charts** | `st.plotly_chart` | `frontend/src/components/PlotlyChart.jsx` (wrapper around `react-plotly.js`) |
| **Analysis Trigger** | `st.button("Analyze")` | `<button onClick={handleAnalyze}>` in `Dashboard.jsx` calling `POST /api/analyze` |
| **Streaming Output** | `st.write_stream` (or iterative `st.write`) | Server-Sent Events (SSE) via `GET /api/stream_analyze` + `EventSource` in React |
| **AI Logic** | Direct calls in `dashboard.py` | `backend/app/inference.py` (encapsulated logic) |
| **State Management** | `st.session_state` | React `useState` / `useEffect` hooks |
| **Styling** | Custom CSS injection | Tailwind CSS classes in JSX |
