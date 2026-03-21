"""Optional LLM adapter for metric and dimension inference fallback."""

from __future__ import annotations

import json
import logging
import re

logger = logging.getLogger(__name__)


def infer_column_with_llm(
    question: str,
    schema_with_types: list[str],
    inference_type: str,
    api_key: str,
) -> str | None:
    """Fallback inference using Claude when deterministic logic is ambiguous."""
    try:
        from anthropic import Anthropic
        from pydantic import BaseModel, Field
    except ImportError:
        logger.debug("Anthropic or Pydantic not installed; skipping LLM fallback.")
        return None

    class ColumnInference(BaseModel):
        selected_column: str = Field(
            ..., description="Exact column name selected from the dataset schema."
        )
        confidence: str = Field(..., description="'high', 'medium', or 'low'")
        reasoning: str = Field(..., description="Brief reasoning.")

    client = Anthropic(api_key=api_key)
    prompt = f"""You are an expert data analyst assistant.
Dataset schema columns (with types): {schema_with_types}
Business question: "{question}"

Task: Identify the single best '{inference_type}' column from the schema to answer the question.
If none apply, leave selected_column blank. Note: return ONLY the base column name, without the type in parentheses.
Respond STRICTLY in valid JSON matching this schema: {{"selected_column": "string", "confidence": "string", "reasoning": "string"}}."""

    try:
        response = client.messages.create(
            model="claude-3-haiku-20240307",
            max_tokens=300,
            timeout=10.0,
            messages=[{"role": "user", "content": prompt}],
        )
        content = str(response.content[0].text)
        json_match = re.search(r"\{.*\}", content, re.DOTALL)
        if json_match:
            data = json.loads(json_match.group())
            inference = ColumnInference(**data)
            # Remove the (type) suffix from the schema list for validation
            valid_columns = [col.split(" (")[0] for col in schema_with_types]
            if inference.selected_column in valid_columns:
                return inference.selected_column
    except Exception as e:
        logger.warning(f"LLM inference failed or timed out: {e}")

    return None
