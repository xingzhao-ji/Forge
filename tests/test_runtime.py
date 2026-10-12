"""Integration checks use the actual scheduler, workers and temporary SQLite files."""
import io
import json
from pathlib import Path
import socket
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from forge import ForgeRuntime, WorkflowValidationError
from forge import api
from forge.executors import BaseExecutor, ForgeTaskError
from forge.models import ExecutionResult


def pipeline():
    return {"name": "synthetic", "idempotency_key": "synthetic-1", "nodes": [
        {"id": "parse", "type": "parse.text.extract", "params": {"text": "alpha beta gamma"}},
        {"id": "count", "type": "text.stats", "depends_on": ["parse"]},
    ]}


class Handler:
    def __init__(self, payload=None):
        data = json.dumps(payload).encode() if payload is not None else b""
        self.headers = {"Content-Length": str(len(data))}
        self.rfile = io.BytesIO(data)
        self.wfile = io.BytesIO()
        self.status = None
        self.payload = None
        self.response_headers = {}

    def send_json(self, payload, status=200, headers=None):
        self.payload, self.status = payload, status

    def send_response(self, status):
        self.status = status

    def send_header(self, name, value):
        self.response_headers[name] = value

    def end_headers(self):
        pass


class RuntimeTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="forge-test-")
        self.addCleanup(self.directory.cleanup)
        # Failing all network connections protects the test's no-provider scope.
        self.network = patch.object(socket.socket, "connect", side_effect=AssertionError("Network forbidden in tests"))
        self.network.start()
        self.addCleanup(self.network.stop)
        self.runtime = ForgeRuntime(self.directory.name)
        self.addCleanup(self.runtime.stop)

    def wait(self, workflow_id, timeout=8):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            detail = self.runtime.get_workflow_detail(workflow_id)
            if detail["workflow"]["status"] in {"completed", "failed", "cancelled"}:
                return detail
            time.sleep(0.02)
        self.fail("Workflow did not reach a terminal state")

    def test_real_dag_api_sse_and_persistence(self):
        self.runtime.start()
        handler = Handler(pipeline())
        api.handle_workflows_create(handler, self.runtime)
        self.assertEqual(handler.status, 201)
        workflow_id = handler.payload["workflow_id"]
        detail = self.wait(workflow_id)
        self.assertEqual(detail["workflow"]["status"], "completed")
        tasks = {t["node_id"]: t for t in detail["tasks"]}
        self.assertEqual(tasks["count"]["outputs"]["words"], 3)
        self.assertEqual(tasks["count"]["depends_on_node_ids"], ["parse"])
        again = Handler(pipeline())
        api.handle_workflows_create(again, self.runtime)
        self.assertEqual(again.status, 200)
        self.assertEqual(again.payload["workflow_id"], workflow_id)
        events = self.runtime.get_workflow_events(workflow_id)
        cursor = events[0]["event_id"]
        stream = Handler()
        stream.headers["Last-Event-ID"] = str(cursor)
        api.handle_workflow_events_stream(stream, self.runtime,
                                         f"/api/workflows/{workflow_id}/events/stream", {})
        body = stream.wfile.getvalue().decode()
        ids = [int(line[4:]) for line in body.splitlines() if line.startswith("id: ")]
        self.assertTrue(ids and all(i > cursor for i in ids))
        self.assertIn("event: workflow_terminal", body)
        self.assertEqual(stream.response_headers["Content-Type"], "text/event-stream; charset=utf-8")
        self.runtime.stop()
        with ForgeRuntime(self.directory.name) as reopened:
            self.assertEqual(reopened.get_workflow_detail(workflow_id)["workflow"]["status"], "completed")
            self.assertEqual(len(reopened.get_workflow_events(workflow_id)), len(events))

    def test_transient_failure_retries_real_worker(self):
        class RetryOnce(BaseExecutor):
            task_type = "test.retry"
            calls = 0

            def execute(self, ctx, task):
                self.calls += 1
                if self.calls == 1:
                    raise ForgeTaskError("synthetic transient error", transient=True)
                return ExecutionResult(outputs={"recovered": True}, artifacts=[], logs=[])

        executor = RetryOnce()
        self.runtime.registry.register(executor)
        self.runtime.start()
        created = self.runtime.create_workflow({"name": "retry fixture", "nodes": [{"id": "retry", "type": executor.task_type}]})
        detail = self.wait(created["workflow_id"])
        self.assertEqual(detail["workflow"]["status"], "completed")
        self.assertEqual(executor.calls, 2)
        self.assertEqual(detail["tasks"][0]["attempt_count"], 2)
        self.assertEqual(len(detail["tasks"][0]["attempts"]), 2)

    def test_cancel_queued_workflow_and_resume(self):
        created = self.runtime.create_workflow(pipeline())
        workflow_id = created["workflow_id"]
        self.assertEqual(self.runtime.cancel_workflow(workflow_id)["status"], "cancelled")
        self.assertTrue(self.runtime.resume_workflow(workflow_id)["changed"])
        self.runtime.start()
        self.assertEqual(self.wait(workflow_id)["workflow"]["status"], "completed")

    def test_four_workers_bound_parallel_execution(self):
        lock = threading.Lock()
        released = threading.Event()
        four_running = threading.Event()
        state = {"running": 0, "peak": 0}

        class Hold(BaseExecutor):
            task_type = "test.hold"

            def execute(self, ctx, task):
                with lock:
                    state["running"] += 1
                    state["peak"] = max(state["peak"], state["running"])
                    if state["running"] == 4:
                        four_running.set()
                try:
                    if not released.wait(5):
                        raise RuntimeError("Test release timed out")
                    return ExecutionResult(outputs={"ok": True}, artifacts=[], logs=[])
                finally:
                    with lock:
                        state["running"] -= 1

        self.runtime.registry.register(Hold())
        self.runtime.start()
        created = self.runtime.create_workflow({"name": "worker-bound fixture", "nodes": [
            {"id": f"task-{i}", "type": "test.hold"} for i in range(9)
        ]})
        try:
            self.assertTrue(four_running.wait(4))
            self.assertEqual(state["peak"], 4)
        finally:
            released.set()
        self.assertEqual(self.wait(created["workflow_id"])["workflow"]["status"], "completed")
        self.assertEqual(state["peak"], 4)

    def test_unknown_provider_task_rejected_before_execution(self):
        with self.assertRaisesRegex(WorkflowValidationError, "[Uu]nsupported"):
            self.runtime.create_workflow({"name": "unsupported fixture", "nodes": [{"id": "external", "type": "image.generate"}]})
        self.assertEqual(set(Path(self.directory.name).iterdir()), {Path(self.directory.name) / "forge.sqlite3"})

    def test_closed_runtime_does_not_silently_restart_dead_worker_pool(self):
        self.runtime.stop()
        self.runtime.stop()
        with self.assertRaises(RuntimeError):
            self.runtime.start()


if __name__ == "__main__":
    unittest.main()
