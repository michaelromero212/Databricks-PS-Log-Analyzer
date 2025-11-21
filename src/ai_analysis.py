from transformers import pipeline
import torch

class AIAnalyzer:
    def __init__(self):
        self.model_name = "distilgpt2" # Small, fast, local-friendly
        self.generator = None
        
    def _load_model(self):
        if not self.generator:
            try:
                print("Loading local AI model (distilgpt2)... this may take a moment.")
                self.generator = pipeline('text-generation', model=self.model_name)
            except Exception as e:
                print(f"Failed to load model: {e}")
                self.generator = None

    def analyze_error(self, error_message):
        """
        Generates a summary/recommendation for a given error.
        """
        if not self.generator:
            self._load_model()
            
        if not self.generator:
            return "AI Model unavailable. Recommendation: Check syntax and table existence."

        prompt = f"Explain this SQL error and suggest a fix: {error_message}\nFix:"
        
        try:
            # Limit output to avoid long generation times
            result = self.generator(prompt, max_length=100, num_return_sequences=1, truncation=True)
            return result[0]['generated_text'].replace(prompt, "").strip()
        except Exception as e:
            return f"AI Analysis failed: {e}"

    def suggest_optimization(self, sql_snippet):
        """
        Suggests optimizations for a SQL snippet.
        """
        if not self.generator:
            self._load_model()
            
        if not self.generator:
            return "AI Model unavailable. Recommendation: Ensure proper indexing and partitioning."

        prompt = f"Optimize this Spark SQL query for performance: {sql_snippet}\nOptimization:"
        
        try:
            result = self.generator(prompt, max_length=150, num_return_sequences=1, truncation=True)
            return result[0]['generated_text'].replace(prompt, "").strip()
        except Exception as e:
            return f"AI Analysis failed: {e}"
