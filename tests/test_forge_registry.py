import copy
import json
from pathlib import Path
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from forge.registry import ForgeRegistry
from forge.routing import ForgeRoutingEngine
from forge.store import ForgeStore


class ForgeRegistryTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.store = ForgeStore(str(self.root / "forge.sqlite3"))
        self.old = {
            "source_name": "builtin", "source_ref": "v1",
            "models": [
                {
                    "modality": "llm", "provider": "openai", "model_id": "legacy-model",
                    "median_latency_ms": 123, "benchmark": {"quality": 0.99},
                    "price_input_per_m": 1, "price_output_per_m": 2,
                },
                {
                    "modality": "parse", "provider": "local", "model_id": "deterministic-parser",
                    "benchmark": {"quality": 0.88},
                },
            ],
        }
        self.new = {
            "source_name": "builtin", "source_ref": "v2",
            "previous_seeds": [{
                "source_name": "builtin", "source_ref": "v1",
                "checksum": ForgeRegistry._compute_checksum(self.old["models"]),
            }],
            "models": [
                {"modality": "llm", "provider": "openai", "model_id": "current-model"},
                {"modality": "parse", "provider": "local", "model_id": "deterministic-parser"},
            ],
        }
        self.path = self.root / "seed.json"
        self.path.write_text(json.dumps(self.new))
        self.registry = ForgeRegistry(self.store, seed_path=str(self.path))

    def test_empty_database_seeds_once_even_with_concurrent_startup(self):
        barrier = threading.Barrier(2)

        def seed():
            barrier.wait()
            return self.registry.ensure_seeded()

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: seed(), range(2)))
        self.assertEqual(sorted(results), [False, True])
        self.assertEqual(len(self.store.list_sync_jobs()), 1)
        self.assertEqual(len(self.registry.list_models()), 2)

    def test_known_seed_refresh_preserves_pins_metrics_and_workflow_history(self):
        self.registry.sync_from_document(self.old)
        created = self.store.create_workflow({
            "name": "pinned workflow",
            "nodes": [{
                "id": "summary", "type": "llm.summarize",
                "params": {"text": "original text", "model": "legacy-model"},
            }],
        }, supported_task_types={"llm.summarize"}, submitted_by="test")
        self.store.claim_runnable_tasks("tester", max_tasks=1, attempt_planner=lambda _: {
            "selected": {"provider": "openai", "model_id": "legacy-model"},
            "score_breakdown": {"quality_proxy": 0.99},
        })
        before = self.store.get_workflow_detail(created["workflow_id"])
        self.assertTrue(self.registry.ensure_seeded())
        self.assertFalse(self.registry.ensure_seeded())
        self.assertEqual(before, self.store.get_workflow_detail(created["workflow_id"]))

        rows = {row["model_id"]: row for row in self.registry.list_models()}
        self.assertIn("seed_pin_only", rows["legacy-model"]["policy_tags"])
        self.assertEqual(rows["legacy-model"]["benchmark"], {"quality": 0.99})
        self.assertEqual(rows["legacy-model"]["source_ref"], "v1")
        self.assertEqual(rows["current-model"]["benchmark"], {})
        self.assertIsNone(rows["current-model"]["median_latency_ms"])
        self.assertEqual(rows["deterministic-parser"]["benchmark"], {})
        refresh = next(job for job in self.store.list_sync_jobs() if job["details"]["source_ref"] == "v2")
        archived = {row["model_id"]: row for row in refresh["details"]["previous_snapshot"]}
        self.assertEqual(json.loads(archived["deterministic-parser"]["benchmark_json"]), {"quality": 0.88})
        self.assertNotIn("seed_pin_only", json.loads(archived["legacy-model"]["policy_tags_json"]))

        routing = ForgeRoutingEngine(self.store)
        with patch.dict("os.environ", {"OPENAI_API_KEY": "test"}, clear=True):
            automatic = routing.preview(task_type="llm.summarize")
            pinned = routing.preview(task_type="llm.summarize", task_params={"model": "legacy-model"})
        self.assertEqual(automatic["selected"]["model_id"], "current-model")
        self.assertEqual(pinned["selected"]["model_id"], "legacy-model")
        self.assertIsNone(automatic["score_breakdown"]["estimated_cost_usd"])
        self.assertIn("pricing", automatic["score_breakdown"]["metric_defaults"])

    def test_manual_registry_is_not_refreshed(self):
        manual = copy.deepcopy(self.old)
        manual.update(source_name="manual", source_ref="operator")
        self.registry.sync_from_document(manual)
        before = self.registry.list_models()
        self.assertFalse(self.registry.ensure_seeded())
        self.assertEqual(self.registry.list_models(), before)
        self.assertEqual(len(self.store.list_sync_jobs()), 1)

    def test_customized_snapshot_with_builtin_provenance_is_not_refreshed(self):
        custom = copy.deepcopy(self.old)
        custom["models"][0]["median_latency_ms"] = 321
        self.registry.sync_from_document(custom)
        before = self.registry.list_models()
        self.assertFalse(self.registry.ensure_seeded())
        self.assertEqual(self.registry.list_models(), before)

    def test_seed_without_declared_predecessor_does_not_replace_existing_models(self):
        self.registry.sync_from_document(self.old)
        self.new.pop("previous_seeds")
        self.path.write_text(json.dumps(self.new))
        self.assertFalse(self.registry.ensure_seeded())
        self.assertIn("legacy-model", {row["model_id"] for row in self.registry.list_models()})



if __name__ == "__main__":
    unittest.main()
