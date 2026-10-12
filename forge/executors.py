"""Executor contracts and registry for Forge."""

from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict

from forge.models import ExecutionResult


class ForgeTaskError(Exception):
    """Structured executor failure used for retry policy."""

    def __init__(self, message: str, *, code: str = "task_error", transient: bool = False):
        super().__init__(message)
        self.code = code
        self.transient = transient


class BaseExecutor:
    task_type = ""

    def validate(self, task: Dict[str, Any]) -> None:
        return None

    def execute(self, ctx: "ExecutionContext", task: Dict[str, Any]) -> ExecutionResult:
        raise NotImplementedError

    def classify_error(self, exc: Exception) -> str:
        if isinstance(exc, ForgeTaskError):
            return "transient" if exc.transient else "permanent"
        if isinstance(exc, TimeoutError):
            return "transient"
        if isinstance(exc, OSError):
            return "transient"
        return "permanent"


class ProviderRouter:
    """Simple single-node provider policy router with lightweight circuit-breaker."""

    def __init__(self):
        self._lock = threading.Lock()
        self._state: Dict[tuple[str, str], Dict[str, Any]] = {}

    def _providers(self, capability: str) -> list[str]:
        providers = []
        if capability in ("llm", "image", "video") and os.environ.get("OPENAI_API_KEY"):
            providers.append("openai")
        if capability in ("llm", "image", "video") and os.environ.get("GEMINI_API_KEY"):
            providers.append("gemini")
        if capability == "llm" and os.environ.get("CLAUDE_API_KEY"):
            providers.append("claude")
        if not providers:
            providers.append("local")
        return providers

    def _order(self, policy: str, providers: list[str]) -> list[str]:
        if policy == "quality_first":
            preferred = ["claude", "openai", "gemini", "local"]
        elif policy == "cost_first":
            preferred = ["local", "gemini", "openai", "claude"]
        else:
            preferred = ["local", "openai", "gemini", "claude"]
        ordered = []
        for name in preferred:
            if name in providers and name not in ordered:
                ordered.append(name)
        for name in providers:
            if name not in ordered:
                ordered.append(name)
        return ordered

    def _is_open(self, capability: str, provider: str) -> bool:
        key = (capability, provider)
        with self._lock:
            state = self._state.get(key)
            if not state:
                return False
            reopen_at = state.get("reopen_at", 0)
            if reopen_at <= time.time():
                state["failures"] = 0
                state["reopen_at"] = 0
                return False
            return True

    def mark_success(self, capability: str, provider: str) -> None:
        key = (capability, provider)
        with self._lock:
            state = self._state.setdefault(key, {"failures": 0, "reopen_at": 0})
            state["failures"] = 0
            state["reopen_at"] = 0

    def mark_transient_failure(self, capability: str, provider: str) -> None:
        key = (capability, provider)
        with self._lock:
            state = self._state.setdefault(key, {"failures": 0, "reopen_at": 0})
            state["failures"] = int(state.get("failures", 0)) + 1
            if state["failures"] >= 3:
                state["reopen_at"] = time.time() + 60

    def pick_provider(
        self,
        capability: str,
        policy: str = "latency_first",
        explicit_provider: str | None = None,
        allow_fallback: bool = True,
    ) -> str:
        providers = self._providers(capability)
        if explicit_provider:
            if explicit_provider not in providers:
                if not allow_fallback:
                    return explicit_provider
            else:
                return explicit_provider

        ordered = self._order(policy, providers)
        for candidate in ordered:
            if not self._is_open(capability, candidate):
                return candidate
        return ordered[0] if ordered else "local"


@dataclass
class ExecutionContext:
    root_dir: str
    provider_router: ProviderRouter = field(default_factory=ProviderRouter)


class ExecutorRegistry:
    """Maps task types to executor instances."""

    def __init__(self):
        self._executors: Dict[str, BaseExecutor] = {}

    def register(self, executor: BaseExecutor) -> None:
        task_type = (executor.task_type or "").strip()
        if not task_type:
            raise ValueError("Executor task_type is required.")
        self._executors[task_type] = executor

    def get(self, task_type: str) -> BaseExecutor:
        if task_type not in self._executors:
            raise KeyError(f"No executor registered for task type: {task_type}")
        return self._executors[task_type]

    def has(self, task_type: str) -> bool:
        return task_type in self._executors

    def task_types(self) -> set[str]:
        return set(self._executors.keys())
