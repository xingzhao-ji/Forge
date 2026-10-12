"""Background scheduler loop for Forge."""

from __future__ import annotations

import threading
from typing import Callable, Dict, Optional

from forge.store import ClaimLimits, ForgeStore
from forge.dispatcher import ForgeDispatcher


class ForgeScheduler:
    def __init__(
        self,
        store: ForgeStore,
        dispatcher: ForgeDispatcher,
        *,
        poll_interval_seconds: float = 0.5,
        lease_ttl_seconds: int = 30,
        max_claims_per_tick: int = 8,
        limits: Optional[ClaimLimits] = None,
        attempt_planner: Optional[Callable[[Dict], Dict]] = None,
    ):
        self.store = store
        self.dispatcher = dispatcher
        self.poll_interval_seconds = max(0.1, float(poll_interval_seconds))
        self.lease_ttl_seconds = max(5, int(lease_ttl_seconds))
        self.max_claims_per_tick = max(1, int(max_claims_per_tick))
        self.limits = limits or ClaimLimits()
        self.attempt_planner = attempt_planner
        self.worker_id = "forge-scheduler"
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, name="forge-scheduler", daemon=True)
        self._thread.start()

    def stop(self, *, timeout: float = 2.0) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)
        self.dispatcher.shutdown(wait=True)

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.store.recover_expired_leases()
                tasks = self.store.claim_runnable_tasks(
                    self.worker_id,
                    limits=self.limits,
                    lease_ttl_seconds=self.lease_ttl_seconds,
                    max_tasks=self.max_claims_per_tick,
                    attempt_planner=self.attempt_planner,
                )
                for task in tasks:
                    self.dispatcher.submit(task)
            except Exception:
                # Keep scheduler alive; detailed failures are stored per-task.
                pass
            self._stop_event.wait(self.poll_interval_seconds)
