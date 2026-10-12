import os
import tempfile
import unittest
from unittest.mock import patch

from forge.routing import ForgeRoutingEngine
from forge.store import ForgeStore


class ForgeRoutingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "data", "forge.sqlite3")
        self.store = ForgeStore(self.db_path)
        self.store.replace_registry_snapshot(
            [
                {
                    "modality": "llm",
                    "provider": "openai",
                    "model_id": "gpt-5.2-codex",
                    "price_input_per_m": 3.0,
                    "price_output_per_m": 12.0,
                    "median_latency_ms": 1800,
                    "throughput_tps": 40,
                    "context_window": 200000,
                    "benchmark": {"reasoning": 0.95, "quality": 0.95},
                    "capabilities": {"json_mode": True},
                    "policy_tags": ["approved"],
                },
                {
                    "modality": "llm",
                    "provider": "gemini",
                    "model_id": "gemini-2.5-pro",
                    "price_input_per_m": 1.0,
                    "price_output_per_m": 4.0,
                    "median_latency_ms": 1300,
                    "throughput_tps": 44,
                    "context_window": 1000000,
                    "benchmark": {"reasoning": 0.89, "quality": 0.89},
                    "capabilities": {"json_mode": True},
                    "policy_tags": ["approved"],
                },
            ],
            source_name="test",
            source_ref="routing",
        )
        self.routing = ForgeRoutingEngine(self.store)

    def tearDown(self):
        self.tmp.cleanup()

    def test_performance_prefers_higher_quality(self):
        with patch.dict(os.environ, {"OPENAI_API_KEY": "x", "GEMINI_API_KEY": "y"}, clear=False):
            plan = self.routing.preview(
                task_type="llm.summarize",
                task_params={"text": "long analysis"},
                routing={"objective": "performance"},
            )
        self.assertEqual(plan.get("objective"), "performance")
        selected = plan.get("selected") or {}
        self.assertEqual(selected.get("provider"), "openai")
        self.assertEqual(selected.get("model_id"), "gpt-5.2-codex")

    def test_cost_prefers_lower_price(self):
        with patch.dict(os.environ, {"OPENAI_API_KEY": "x", "GEMINI_API_KEY": "y"}, clear=False):
            plan = self.routing.preview(
                task_type="llm.summarize",
                task_params={"text": "short summary"},
                routing={"objective": "cost"},
            )
        selected = plan.get("selected") or {}
        self.assertEqual(selected.get("provider"), "gemini")
        self.assertEqual(selected.get("model_id"), "gemini-2.5-pro")

    def test_policy_filter_respects_allowed_providers(self):
        with patch.dict(os.environ, {"OPENAI_API_KEY": "x", "GEMINI_API_KEY": "y"}, clear=False):
            plan = self.routing.preview(
                task_type="llm.summarize",
                task_params={"text": "policy constrained"},
                routing={
                    "objective": "policy",
                    "constraints": {"allowed_providers": ["gemini"]},
                },
            )
        selected = plan.get("selected") or {}
        self.assertEqual(selected.get("provider"), "gemini")

    def test_open_circuit_provider_is_excluded(self):
        self.store.record_provider_failure(provider="openai", modality="llm", transient=True)
        self.store.record_provider_failure(provider="openai", modality="llm", transient=True)
        self.store.record_provider_failure(provider="openai", modality="llm", transient=True)
        with patch.dict(os.environ, {"OPENAI_API_KEY": "x", "GEMINI_API_KEY": "y"}, clear=False):
            plan = self.routing.preview(
                task_type="llm.summarize",
                task_params={"text": "fallback test"},
                routing={"objective": "performance"},
            )
        selected = plan.get("selected") or {}
        self.assertEqual(selected.get("provider"), "gemini")

    def test_missing_prices_remain_unknown_and_do_not_beat_free_models(self):
        for modality in ("llm", "image", "video"):
            with self.subTest(modality=modality):
                score, estimate = self.routing._cost_score(modality=modality, prompt_size=100, row={})
                self.assertEqual(score, 0.5)
                self.assertIsNone(estimate)
                local = self.routing._local_fallback_candidate(modality, {})
                free_score, free_estimate = self.routing._cost_score(modality=modality, prompt_size=100, row=local)
                self.assertEqual(free_score, 1)
                self.assertEqual(free_estimate, 0)

    def test_unknown_metrics_are_identified_in_routing_preview(self):
        self.store.replace_registry_snapshot([
            {"modality": "llm", "provider": "openai", "model_id": "unknown-pricing"},
            {"modality": "llm", "provider": "openai", "model_id": "known-pricing",
             "price_input_per_m": 1, "price_output_per_m": 2},
        ])
        with patch.dict(os.environ, {"OPENAI_API_KEY": "test"}, clear=True):
            plan = self.routing.preview(task_type="llm.summarize", routing={"objective": "cost"})
        self.assertEqual(plan["selected"]["model_id"], "known-pricing")
        self.assertEqual(plan["score_breakdown"]["latency_score"], 0.5)
        self.assertEqual(plan["score_breakdown"]["throughput_score"], 0.5)
        self.assertIn("median_latency_ms", plan["score_breakdown"]["metric_defaults"])


if __name__ == "__main__":
    unittest.main()
