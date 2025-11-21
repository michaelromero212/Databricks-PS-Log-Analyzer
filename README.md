# Databricks PS Log Analyzer

**Automated Root Cause Analysis & Optimization for Databricks Workloads**

![Dashboard Preview](docs/images/dashboard_preview.png)

## 🚀 Overview

Consultants often lose hours triaging logs and query failures. This tool automates the first-pass root cause analysis by:
1.  **Parsing Logs**: Instantly identifying error types and stack traces.
2.  **Analyzing SQL**: Detecting common anti-patterns (Cartesian joins, full table scans).
3.  **AI Recommendations**: Using local LLMs to suggest fixes and optimizations.

## 🛠️ Setup

1.  **Create Virtual Environment**:
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

2.  **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

3.  **Configure Environment**:
    Copy `.env.example` to `.env` and add your Databricks credentials (optional for mock mode).
    ```bash
    cp .env.example .env
    ```

## 📊 Running the Dashboard

Launch the interactive analysis dashboard:

```bash
streamlit run src/dashboard.py
```

## 💻 CLI Usage

Run analysis from the command line:

```bash
# Run on local sample data
python src/run_analysis.py --local-sample

# Run on latest Databricks jobs (requires .env)
python src/run_analysis.py --latest
```

## 🧠 How It Works

-   **Log Parser**: Scans JSON output from Databricks Jobs API for failure states and spill-to-disk metrics.
-   **SQL Rules**: Uses static analysis to catch `SELECT *`, missing `WHERE` clauses, and cross joins.
-   **AI Engine**: Uses `distilgpt2` (running locally) to generate human-readable summaries of complex Java/Spark stack traces.

## 🔮 Future Roadmap

-   Integration with Databricks Repos for direct code fixes.
-   Auto-generation of Pull Requests with suggested optimizations.
-   Support for more advanced LLMs (Llama 2, Mistral) for deeper code analysis.
