"""Local deterministic tasks; no network or private workspace access."""
from typing import Any, Dict

from forge.executors import BaseExecutor, ForgeTaskError
from forge.models import ExecutionResult

class ParseTextExtractExecutor(BaseExecutor):
    task_type = "parse.text.extract"

    def execute(self, ctx, task: Dict[str, Any]) -> ExecutionResult:
        params = task.get("params") or {}
        text = (params.get("text") or "").strip()
        if not text:
            upstream = task.get("upstream") or {}
            for payload in upstream.values():
                if isinstance(payload, dict) and isinstance(payload.get("text"), str):
                    text = payload["text"].strip()
                    if text:
                        break
        if not text:
            raise ForgeTaskError("parse.text.extract requires text input", code="missing_text")
        outputs = {
            "text": text,
            "chars": len(text),
            "parser": "deterministic",
        }
        logs = [f"parse.text.extract chars={len(text)}"]
        return ExecutionResult(outputs=outputs, artifacts=[], logs=logs, provider_used="local")


class TextStatsExecutor(BaseExecutor):
    """Small standalone example; this does not perform model inference."""

    task_type = "text.stats"

    def execute(self, ctx, task: Dict[str, Any]) -> ExecutionResult:
        text = (task.get("params") or {}).get("text")
        if text is None:
            for payload in (task.get("upstream") or {}).values():
                if isinstance(payload, dict) and isinstance(payload.get("text"), str):
                    text = payload["text"]
                    break
        if not isinstance(text, str):
            raise ForgeTaskError("text.stats requires text input", code="missing_text")
        return ExecutionResult(
            outputs={"characters": len(text), "words": len(text.split()),
                     "lines": len(text.splitlines())},
            artifacts=[], logs=["Counted text locally."], provider_used="local",
        )
