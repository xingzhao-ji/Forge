"""Core models and helpers for Forge workflow runtime."""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List


WORKFLOW_STATUS_QUEUED = "queued"
WORKFLOW_STATUS_RUNNING = "running"
WORKFLOW_STATUS_COMPLETED = "completed"
WORKFLOW_STATUS_FAILED = "failed"
WORKFLOW_STATUS_CANCEL_REQUESTED = "cancel_requested"
WORKFLOW_STATUS_CANCELLED = "cancelled"

WORKFLOW_TERMINAL_STATUSES = {
    WORKFLOW_STATUS_COMPLETED,
    WORKFLOW_STATUS_FAILED,
    WORKFLOW_STATUS_CANCELLED,
}


TASK_STATUS_QUEUED = "queued"
TASK_STATUS_RUNNING = "running"
TASK_STATUS_RETRY_WAIT = "retry_wait"
TASK_STATUS_SUCCEEDED = "succeeded"
TASK_STATUS_FAILED = "failed"
TASK_STATUS_CANCELLED = "cancelled"
TASK_STATUS_BLOCKED = "blocked"

TASK_TERMINAL_STATUSES = {
    TASK_STATUS_SUCCEEDED,
    TASK_STATUS_FAILED,
    TASK_STATUS_CANCELLED,
    TASK_STATUS_BLOCKED,
}


@dataclass(frozen=True)
class ExecutionResult:
    """Standard executor return payload."""

    outputs: Dict[str, Any]
    artifacts: List[Dict[str, Any]]
    logs: List[str]
    provider_used: str = "local"
    cost_usd: float = 0.0


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(ts: datetime | None = None) -> str:
    value = ts or now_utc()
    return value.replace(microsecond=0).isoformat()


def parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def payload_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def next_backoff_seconds(attempt_no: int) -> int:
    # attempt_no starts at 1 on first try
    bounded = max(1, attempt_no)
    return min(300, 2 ** bounded)


def workflow_id() -> str:
    stamp = now_utc().strftime("%Y%m%d_%H%M%S")
    return f"wf_{stamp}_{uuid.uuid4().hex[:8]}"


def task_id() -> str:
    return f"tsk_{uuid.uuid4().hex[:12]}"


def attempt_id() -> str:
    return f"att_{uuid.uuid4().hex[:12]}"


def artifact_id() -> str:
    return f"art_{uuid.uuid4().hex[:12]}"


def plus_seconds(ts: datetime, seconds: int) -> datetime:
    return ts + timedelta(seconds=seconds)
