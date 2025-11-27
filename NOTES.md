# Model Selection Notes

We selected specific Hugging Face models to balance performance, cost (free), and ease of local deployment.

*   **`sentence-transformers/all-MiniLM-L6-v2` (Embeddings)**
    *   **Why**: Extremely fast and lightweight (80MB).
    *   **Trade-off**: Lower semantic nuance than `e5-large` or OpenAI embeddings, but sufficient for log clustering.
    *   **Use Case**: Grouping similar error logs to identify patterns.

*   **`google/flan-t5-small` (Generation)**
    *   **Why**: Runs easily on CPUs (approx 300MB RAM), follows instructions well for its size.
    *   **Trade-off**: Context window is small; cannot handle massive stack traces in one go. Output can be terse.
    *   **Use Case**: Generating root cause summaries and simple fix suggestions.

*   **`facebook/bart-large-cnn` (Alternative)**
    *   **Why**: Better for pure summarization of long text.
    *   **Trade-off**: Larger (1.6GB), slower on CPU.
    *   **Use Case**: If `flan-t5` summaries are too abstract, switch to this.
