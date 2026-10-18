"""Worker dispatcher for Forge claimed tasks."""

from __future__ import annotations

import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from typing import Any, Dict

from forge.executors import ExecutorRegistry
from forge.models import iso_utc, next_backoff_seconds, now_utc
from forge.store import ForgeStore


def _task_modality(task_type: str) -> str:
    value = (task_type or "").strip().lower()
    if value.startswith("llm."):
        return "llm"
    if value.startswith("image.generate"):
        return "image"
    if value.startswith("video.generate"):
        return "video"
    if value.startswith("parse.") or value == "notes.extract":
        return "parse"
    return "default"


class ForgeDispatcher:
    def __init__(
        self,
        store: ForgeStore,
        registry: ExecutorRegistry,
        context,
        *,
        max_workers: int = 4,
    ):
        self.store = store
        self.registry = registry
        self.context = context
        self.pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="forge-worker")

    def submit(self, task: Dict[str, Any]) -> None:
        self.pool.submit(self._run_task, task)

    def _run_task(self, claimed_task: Dict[str, Any]) -> None:
        task_id_value = claimed_task["task_id"]
        loaded = self.store.load_task_for_execution(task_id_value)
        if not loaded or loaded["status"] != "running" or loaded["attempt_count"] != claimed_task["attempt_count"]:
            return

        workflow_id_value = loaded["workflow_id"]
        modality = _task_modality(loaded["task_type"])
        attempt_plan = loaded.get("attempt_plan") if isinstance(loaded.get("attempt_plan"), dict) else {}
        selected = attempt_plan.get("selected") if isinstance(attempt_plan, dict) else {}
        planned_provider = (selected.get("provider") or loaded.get("attempt_provider") or "").strip().lower()
        planned_model = (selected.get("model_id") or loaded.get("attempt_model") or "").strip()
        planned_objective = (attempt_plan.get("objective") or loaded.get("attempt_objective") or "").strip().lower()
        planned_breakdown = attempt_plan.get("score_breakdown") if isinstance(attempt_plan, dict) else {}
        if not isinstance(planned_breakdown, dict):
            planned_breakdown = {}

        executor = self.registry.get(loaded["task_type"])
        try:
            executor.validate(loaded)
            result = executor.execute(self.context, loaded)

            actual_provider = (
                (result.provider_used or "").strip().lower()
                or (result.outputs.get("provider") if isinstance(result.outputs, dict) else "")
                or planned_provider
            )
            actual_model = (
                (result.outputs.get("model") if isinstance(result.outputs, dict) else "")
                or planned_model
            )
            provider_job_id = ""
            if isinstance(result.outputs, dict):
                provider_job_id = (
                    str(result.outputs.get("provider_job_id") or "")
                    or str(result.outputs.get("video_id") or "")
                    or str(result.outputs.get("job_id") or "")
                )
            fallback_used = bool(
                planned_provider
                and actual_provider
                and planned_provider != actual_provider
            )

            self.store.complete_task(
                task_id_value,
                expected_attempt_no=claimed_task["attempt_count"],
                outputs=result.outputs,
                artifacts=result.artifacts,
                logs=result.logs,
                provider_used=actual_provider or result.provider_used,
                model_used=actual_model,
                objective=planned_objective,
                score_breakdown=planned_breakdown,
                fallback_used=fallback_used,
                provider_job_id=provider_job_id,
                cost_usd=float(result.cost_usd or 0.0),
            )
            if actual_provider:
                self.store.record_provider_success(provider=actual_provider, modality=modality)
                cap = "llm" if loaded["task_type"].startswith("llm.") else (
                    "image" if loaded["task_type"].startswith("image.generate") else (
                        "video" if loaded["task_type"].startswith("video.generate") else "default"
                    )
                )
                self.context.provider_router.mark_success(cap, actual_provider)
        except Exception as exc:  # intentionally broad for durable worker loop
            kind = executor.classify_error(exc)
            error_payload = {
                "code": getattr(exc, "code", exc.__class__.__name__.lower()),
                "message": str(exc),
                "type": exc.__class__.__name__,
                "traceback": traceback.format_exc(limit=4),
            }
            policy_cap = "llm" if loaded["task_type"].startswith("llm.") else (
                "image" if loaded["task_type"].startswith("image.generate") else (
                    "video" if loaded["task_type"].startswith("video.generate") else "default"
                )
            )
            provider = planned_provider
            if not provider and isinstance(loaded.get("params"), dict):
                provider = (
                    loaded["params"].get("explicit_provider")
                    or loaded["params"].get("provider")
                    or ""
                ).strip().lower()

            if kind == "transient" and provider:
                self.store.record_provider_failure(
                    provider=provider,
                    modality=modality,
                    transient=True,
                )
                self.context.provider_router.mark_transient_failure(policy_cap, provider)

            if kind == "transient" and int(loaded["attempt_count"]) < int(loaded["max_attempts"]):
                wait = next_backoff_seconds(int(loaded["attempt_count"]))
                next_run = iso_utc(now_utc() + timedelta(seconds=wait))
                self.store.retry_task(
                    task_id_value,
                    expected_attempt_no=claimed_task["attempt_count"],
                    error=error_payload,
                    next_run_at=next_run,
                    logs=[error_payload["message"]],
                    provider_used=provider,
                    model_used=planned_model,
                    objective=planned_objective,
                    score_breakdown=planned_breakdown,
                )
            elif kind == "cancelled":
                self.store.cancel_task(
                    task_id_value,
                    expected_attempt_no=claimed_task["attempt_count"],
                    reason=error_payload["message"],
                    provider_used=provider,
                    model_used=planned_model,
                    objective=planned_objective,
                    score_breakdown=planned_breakdown,
                )
            else:
                if provider:
                    self.store.record_provider_failure(
                        provider=provider,
                        modality=modality,
                        transient=(kind == "transient"),
                    )
                self.store.fail_task(
                    task_id_value,
                    expected_attempt_no=claimed_task["attempt_count"],
                    error=error_payload,
                    logs=[error_payload["message"]],
                    provider_used=provider,
                    model_used=planned_model,
                    objective=planned_objective,
                    score_breakdown=planned_breakdown,
                )
        finally:
            self.store.refresh_workflow_state(workflow_id_value)

    def shutdown(self, *, wait: bool = True) -> None:
        self.pool.shutdown(wait=wait, cancel_futures=False)
