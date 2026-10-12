import os
import tempfile
import unittest
from datetime import timedelta

from forge.models import TASK_STATUS_FAILED, TASK_STATUS_RETRY_WAIT, iso_utc, now_utc
from forge.store import ForgeStore, WorkflowValidationError


class ForgeStoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "data", "forge.sqlite3")
        self.store = ForgeStore(self.db_path)
        self.supported = {
            "notes.extract",
            "parse.text.extract",
            "parse.pdf.extract",
            "llm.summarize",
            "llm.key_questions",
            "artifact.save_note",
        }

    def tearDown(self):
        self.tmp.cleanup()

    def test_create_workflow_persists_graph(self):
        payload = {
            "name": "research",
            "idempotency_key": "research-1",
            "nodes": [
                {"id": "parse", "type": "notes.extract", "params": {"note_id": "abc12345"}},
                {"id": "summary", "type": "llm.summarize", "depends_on": ["parse"]},
                {"id": "persist", "type": "artifact.save_note", "depends_on": ["summary"]},
            ],
        }
        result = self.store.create_workflow(payload, supported_task_types=self.supported, submitted_by="test")
        self.assertTrue(result["created"])
        detail = self.store.get_workflow_detail(result["workflow_id"])
        self.assertIsNotNone(detail)
        self.assertEqual(len(detail["tasks"]), 3)
        by_node = {task["node_id"]: task for task in detail["tasks"]}
        self.assertEqual(by_node["summary"]["depends_on_node_ids"], ["parse"])
        self.assertEqual(by_node["persist"]["depends_on_node_ids"], ["summary"])

    def test_idempotency_returns_existing_workflow(self):
        payload = {
            "name": "simple",
            "idempotency_key": "same-key",
            "nodes": [{"id": "n1", "type": "llm.summarize", "params": {"text": "hello"}}],
        }
        first = self.store.create_workflow(payload, supported_task_types=self.supported, submitted_by="test")
        second = self.store.create_workflow(payload, supported_task_types=self.supported, submitted_by="test")
        self.assertTrue(first["created"])
        self.assertFalse(second["created"])
        self.assertEqual(first["workflow_id"], second["workflow_id"])

    def test_cycle_validation_rejected(self):
        payload = {
            "name": "cycle",
            "nodes": [
                {"id": "a", "type": "llm.summarize", "depends_on": ["b"], "params": {"text": "x"}},
                {"id": "b", "type": "llm.key_questions", "depends_on": ["a"]},
            ],
        }
        with self.assertRaises(WorkflowValidationError):
            self.store.create_workflow(payload, supported_task_types=self.supported, submitted_by="test")

    def test_retry_and_fail_transitions(self):
        payload = {
            "name": "retry-flow",
            "nodes": [{"id": "summary", "type": "llm.summarize", "params": {"text": "hello"}}],
        }
        created = self.store.create_workflow(payload, supported_task_types=self.supported, submitted_by="test")
        wf_id = created["workflow_id"]
        claimed = self.store.claim_runnable_tasks("tester", max_tasks=1)
        self.assertEqual(len(claimed), 1)
        task_id = claimed[0]["task_id"]

        self.store.retry_task(
            task_id,
            error={"code": "timeout", "message": "transient"},
            next_run_at=iso_utc(),
        )
        detail = self.store.get_workflow_detail(wf_id)
        only_task = detail["tasks"][0]
        self.assertEqual(only_task["status"], TASK_STATUS_RETRY_WAIT)

        claimed_again = self.store.claim_runnable_tasks("tester", max_tasks=1)
        self.assertEqual(len(claimed_again), 1)
        self.store.fail_task(task_id, error={"code": "fatal", "message": "permanent"})
        detail = self.store.get_workflow_detail(wf_id)
        self.assertEqual(detail["tasks"][0]["status"], TASK_STATUS_FAILED)
        self.assertEqual(detail["workflow"]["status"], "failed")

    def test_budget_guardrail_marks_workflow_failed(self):
        payload = {
            "name": "budget-tight",
            "budget_usd": 0.0001,
            "nodes": [{"id": "summary", "type": "llm.summarize", "params": {"text": "hello budget"}}],
        }
        created = self.store.create_workflow(payload, supported_task_types=self.supported, submitted_by="test")
        wf_id = created["workflow_id"]
        claimed = self.store.claim_runnable_tasks("tester", max_tasks=1)
        self.assertEqual(claimed, [])
        detail = self.store.get_workflow_detail(wf_id)
        self.assertEqual(detail["workflow"]["status"], "failed")
        self.assertEqual(detail["tasks"][0]["status"], "failed")
        self.assertGreaterEqual(len(detail["events"]), 1)
        failure_payloads = [evt.get("payload") or {} for evt in detail["events"] if evt.get("event_type") == "task.failed"]
        self.assertTrue(any((item.get("error") or {}).get("code") == "budget_exceeded" for item in failure_payloads))

    def test_artifact_blob_ref_count_increments_on_duplicate_hash(self):
        payload1 = {
            "name": "blob-1",
            "nodes": [{"id": "summary", "type": "llm.summarize", "params": {"text": "a"}}],
        }
        payload2 = {
            "name": "blob-2",
            "nodes": [{"id": "summary", "type": "llm.summarize", "params": {"text": "b"}}],
        }
        wf1 = self.store.create_workflow(payload1, supported_task_types=self.supported, submitted_by="test")
        wf2 = self.store.create_workflow(payload2, supported_task_types=self.supported, submitted_by="test")

        claim1 = self.store.claim_runnable_tasks("tester", max_tasks=1)[0]
        first_claim_workflow_id = claim1["workflow_id"]
        self.store.complete_task(
            claim1["task_id"],
            outputs={"text": "done"},
            artifacts=[{
                "artifact_type": "image",
                "path_or_uri": "/Creations/a.png",
                "sha256": "deadbeef",
                "bytes": 128,
                "metadata": {"x": 1},
            }],
            logs=["ok"],
            provider_used="local",
        )

        claim2 = self.store.claim_runnable_tasks("tester", max_tasks=1)[0]
        second_claim_workflow_id = claim2["workflow_id"]
        self.store.complete_task(
            claim2["task_id"],
            outputs={"text": "done"},
            artifacts=[{
                "artifact_type": "image",
                "path_or_uri": "/Creations/b.png",
                "sha256": "deadbeef",
                "bytes": 128,
                "metadata": {"x": 2},
            }],
            logs=["ok"],
            provider_used="local",
        )

        conn = self.store._connect()
        try:
            row = conn.execute(
                "SELECT ref_count, canonical_path FROM artifact_blobs WHERE sha256 = ?",
                ("deadbeef",),
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(int(row["ref_count"]), 2)
            self.assertEqual(row["canonical_path"], "/Creations/a.png")
        finally:
            conn.close()

        self.assertNotEqual(first_claim_workflow_id, second_claim_workflow_id)
        detail_second = self.store.get_workflow_detail(second_claim_workflow_id)
        metadata = (detail_second["artifacts"][0] or {}).get("metadata") or {}
        self.assertTrue(metadata.get("deduped"))
        self.assertGreaterEqual(int(metadata.get("blob_ref_count") or 0), 2)

    def test_priority_aging_promotes_old_high_priority_value(self):
        first = self.store.create_workflow(
            {
                "name": "old-task",
                "nodes": [{"id": "old", "type": "llm.summarize", "priority": 100, "params": {"text": "x"}}],
            },
            supported_task_types=self.supported,
            submitted_by="test",
        )
        second = self.store.create_workflow(
            {
                "name": "new-task",
                "nodes": [{"id": "new", "type": "llm.summarize", "priority": 10, "params": {"text": "y"}}],
            },
            supported_task_types=self.supported,
            submitted_by="test",
        )

        old_iso = iso_utc(now_utc() - timedelta(hours=2))
        conn = self.store._connect()
        try:
            conn.execute(
                """
                UPDATE tasks
                SET created_at = ?, next_run_at = ?
                WHERE workflow_id = ?
                """,
                (old_iso, old_iso, first["workflow_id"]),
            )
            conn.commit()
        finally:
            conn.close()

        claimed = self.store.claim_runnable_tasks("tester", max_tasks=1)
        self.assertEqual(len(claimed), 1)
        self.assertEqual(claimed[0]["workflow_id"], first["workflow_id"])
        self.assertNotEqual(claimed[0]["workflow_id"], second["workflow_id"])

    def test_routing_objective_maps_provider_policy(self):
        payload = {
            "name": "routing-objective",
            "routing": {
                "objective": "cost",
                "constraints": {"max_cost_usd": 0.4, "max_latency_ms": 12000},
            },
            "nodes": [{"id": "summary", "type": "llm.summarize", "params": {"text": "hello"}}],
        }
        created = self.store.create_workflow(payload, supported_task_types=self.supported, submitted_by="test")
        detail = self.store.get_workflow_detail(created["workflow_id"])
        self.assertIsNotNone(detail)
        workflow = detail["workflow"]
        self.assertAlmostEqual(float(workflow.get("budget_usd") or 0), 0.4, places=4)
        params = detail["tasks"][0]["params"]
        self.assertEqual(params.get("provider_policy"), "cost_first")
        routing = params.get("_routing") or {}
        self.assertEqual(routing.get("objective"), "cost")
        self.assertEqual((routing.get("constraints") or {}).get("max_latency_ms"), 12000)

    def test_invalid_routing_objective_rejected(self):
        payload = {
            "name": "bad-routing",
            "routing": {"objective": "unknown"},
            "nodes": [{"id": "summary", "type": "llm.summarize", "params": {"text": "x"}}],
        }
        with self.assertRaises(WorkflowValidationError):
            self.store.create_workflow(payload, supported_task_types=self.supported, submitted_by="test")

    def test_node_routing_override_can_be_top_level(self):
        payload = {
            "name": "node-routing-override",
            "routing": {"objective": "cost"},
            "nodes": [
                {
                    "id": "summary",
                    "type": "llm.summarize",
                    "params": {"text": "hello"},
                    "routing_override": {"objective": "performance", "require_json": True},
                }
            ],
        }
        created = self.store.create_workflow(payload, supported_task_types=self.supported, submitted_by="test")
        detail = self.store.get_workflow_detail(created["workflow_id"])
        params = (detail["tasks"][0] or {}).get("params") or {}
        routing = params.get("_routing") or {}
        self.assertEqual(routing.get("objective"), "performance")
        self.assertTrue((routing.get("constraints") or {}).get("require_json"))
        self.assertEqual(params.get("provider_policy"), "quality_first")

    def test_claim_persists_attempt_plan_metadata(self):
        payload = {
            "name": "attempt-plan",
            "nodes": [{"id": "summary", "type": "llm.summarize", "params": {"text": "hello"}}],
        }
        created = self.store.create_workflow(payload, supported_task_types=self.supported, submitted_by="test")
        wf_id = created["workflow_id"]

        claimed = self.store.claim_runnable_tasks(
            "planner",
            max_tasks=1,
            attempt_planner=lambda _: {
                "objective": "performance",
                "candidate_count": 2,
                "selected": {"provider": "openai", "model_id": "gpt-5.2-codex"},
                "fallback_chain": [{"provider": "gemini", "model_id": "gemini-2.5-pro", "score": 0.9}],
                "score_breakdown": {"final_score": 0.95, "quality_proxy": 0.9},
            },
        )
        self.assertEqual(len(claimed), 1)
        detail = self.store.get_workflow_detail(wf_id)
        task = detail["tasks"][0]
        latest = task.get("latest_attempt") or {}
        self.assertEqual(latest.get("provider_used"), "openai")
        self.assertEqual(latest.get("model_used"), "gpt-5.2-codex")
        self.assertEqual(latest.get("objective"), "performance")
        self.assertEqual(int(latest.get("candidate_count") or 0), 2)
        attempts = task.get("attempts") or []
        self.assertEqual(len(attempts), 1)
        self.assertEqual((attempts[0].get("score_breakdown") or {}).get("quality_proxy"), 0.9)


if __name__ == "__main__":
    unittest.main()
