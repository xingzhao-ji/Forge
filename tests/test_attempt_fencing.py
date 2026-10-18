"""Late workers must not close or overwrite a newer persisted attempt."""
import sqlite3
import tempfile
import unittest
from datetime import timedelta

from forge import ForgeRuntime
from forge.executors import BaseExecutor, ForgeTaskError
from forge.models import ExecutionResult, iso_utc, now_utc


class AttemptFencingTests(unittest.TestCase):
    def test_duplicate_completion_preserves_output_cost_and_events(self):
        with tempfile.TemporaryDirectory() as directory, ForgeRuntime(directory) as runtime:
            # Keep the background scheduler out of these deterministic store transitions.
            runtime.scheduler._stop_event.set()
            runtime.scheduler._thread.join(timeout=2)
            workflow = runtime.create_workflow({"name": "duplicate fixture", "nodes": [
                {"id": "count", "type": "text.stats", "params": {"text": "fixture"}}
            ]})
            task = runtime.store.claim_runnable_tasks("fixture", max_tasks=1)[0]
            runtime.store.complete_task(task["task_id"], outputs={"original": True}, cost_usd=0.25)
            before = runtime.get_workflow_detail(workflow["workflow_id"])
            runtime.store.complete_task(task["task_id"], outputs={"late": True}, cost_usd=0.5)
            self.assertEqual(runtime.get_workflow_detail(workflow["workflow_id"]), before)

    def test_late_worker_results_do_not_modify_reclaimed_attempt(self):
        for outcome in ("success", "transient", "permanent", "cancelled"):
            with self.subTest(outcome=outcome), tempfile.TemporaryDirectory() as directory:
                runtime = ForgeRuntime(directory)
                try:
                    class ExpiredWorker(BaseExecutor):
                        task_type = "test.expired"

                        def classify_error(self, exc):
                            return outcome

                        def execute(self, ctx, task):
                            runtime.store.recover_expired_leases(
                                now=iso_utc(now_utc() + timedelta(hours=1)))
                            with sqlite3.connect(runtime.db_path) as conn:
                                conn.execute("UPDATE tasks SET next_run_at = ? WHERE task_id = ?",
                                             ("2000-01-01T00:00:00+00:00", task["task_id"]))
                            claimed = runtime.store.claim_runnable_tasks("replacement", max_tasks=1,
                                attempt_planner=lambda task: {"selected": {}})
                            self_test.assertEqual(claimed[0]["attempt_count"], 2)
                            if outcome != "success":
                                raise ForgeTaskError("late worker error")
                            return ExecutionResult(outputs={"stale": True}, artifacts=[], logs=[], cost_usd=0.25)

                    self_test = self
                    runtime.registry.register(ExpiredWorker())
                    workflow = runtime.create_workflow({"name": "expired fixture", "nodes": [{"id": "job", "type": "test.expired"}]})
                    original = runtime.store.claim_runnable_tasks("original", max_tasks=1,
                        attempt_planner=lambda task: {"selected": {}})[0]
                    runtime.dispatcher._run_task(original)
                    task = runtime.get_workflow_detail(workflow["workflow_id"])["tasks"][0]
                    self.assertEqual(task["status"], "running")
                    self.assertEqual(task["outputs"], {})
                    self.assertEqual([attempt["status"] for attempt in task["attempts"]], ["abandoned", "running"])
                finally:
                    runtime.stop()


if __name__ == "__main__":
    unittest.main()
