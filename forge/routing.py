"""Objective-based routing engine for Forge."""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Tuple

from forge.store import ForgeStore


OBJECTIVE_DEFAULT = "balanced"
OBJECTIVES = {"balanced", "performance", "cost", "fastest", "policy"}


def infer_modality(task_type: str) -> str:
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


def _merge_routing(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    merged: Dict[str, Any] = {}
    base_obj = base if isinstance(base, dict) else {}
    override_obj = override if isinstance(override, dict) else {}
    objective = (override_obj.get("objective") or base_obj.get("objective") or OBJECTIVE_DEFAULT).strip().lower()
    if objective not in OBJECTIVES:
        objective = OBJECTIVE_DEFAULT
    merged["objective"] = objective
    profile = (override_obj.get("policy_profile") or base_obj.get("policy_profile") or "default").strip().lower()
    merged["policy_profile"] = profile or "default"
    constraints = {}
    if isinstance(base_obj.get("constraints"), dict):
        constraints.update(base_obj.get("constraints") or {})
    if isinstance(override_obj.get("constraints"), dict):
        constraints.update(override_obj.get("constraints") or {})
    if base_obj.get("max_total_cost_usd") not in (None, ""):
        constraints.setdefault("max_cost_usd", base_obj.get("max_total_cost_usd"))
    if override_obj.get("max_total_cost_usd") not in (None, ""):
        constraints["max_cost_usd"] = override_obj.get("max_total_cost_usd")
    if "prefer_cached_registry" in base_obj:
        merged["prefer_cached_registry"] = bool(base_obj.get("prefer_cached_registry"))
    if "prefer_cached_registry" in override_obj:
        merged["prefer_cached_registry"] = bool(override_obj.get("prefer_cached_registry"))
    merged["constraints"] = constraints
    return merged


class ForgeRoutingEngine:
    def __init__(self, store: ForgeStore):
        self.store = store

    @staticmethod
    def _provider_available(provider: str, modality: str) -> bool:
        name = (provider or "").strip().lower()
        if name == "openai":
            return bool(os.environ.get("OPENAI_API_KEY"))
        if name == "gemini":
            return bool(os.environ.get("GEMINI_API_KEY"))
        if name == "claude":
            return bool(os.environ.get("CLAUDE_API_KEY")) and modality == "llm"
        if name == "local":
            return True
        return True

    @staticmethod
    def _policy_allowed(policy_tags: List[str], profile: str) -> bool:
        tags = {str(item).strip().lower() for item in (policy_tags or []) if str(item).strip()}
        if profile in ("", "default"):
            return True
        if profile == "internal":
            return "internal_only" in tags or "internal" in tags
        if profile == "no_external":
            return "external" not in tags
        if profile == "pii_safe":
            return "no_pii" in tags or "pii_allowed" not in tags
        return True

    @staticmethod
    def _task_class(task_type: str, modality: str) -> str:
        lower = (task_type or "").lower()
        if "code" in lower:
            return "coding"
        if modality in ("image", "video"):
            return "vision"
        if "classify" in lower or "extract" in lower:
            return "classification"
        return "reasoning"

    @staticmethod
    def _quality_proxy(benchmark: Dict[str, Any], task_class: str) -> float:
        if not isinstance(benchmark, dict):
            return 0.55
        keys = {
            "coding": ("coding", "code", "quality"),
            "vision": ("vision", "video", "image", "quality"),
            "classification": ("classification", "reasoning", "quality"),
            "reasoning": ("reasoning", "quality"),
        }.get(task_class, ("quality",))
        for key in keys:
            value = benchmark.get(key)
            if isinstance(value, (int, float)):
                return max(0.0, min(1.0, float(value)))
        for value in benchmark.values():
            if isinstance(value, (int, float)):
                return max(0.0, min(1.0, float(value)))
        return 0.55

    @staticmethod
    def _latency_score(latency_ms: Any) -> float:
        try:
            latency = float(latency_ms)
        except Exception:
            return 0.5
        latency = max(1.0, latency)
        return max(0.0, min(1.0, 1.0 - min(latency, 60000.0) / 60000.0))

    @staticmethod
    def _throughput_score(value: Any) -> float:
        try:
            tps = float(value)
        except Exception:
            return 0.5
        return max(0.0, min(1.0, tps / 120.0))

    @staticmethod
    def _cost_score(
        *,
        modality: str,
        prompt_size: int,
        row: Dict[str, Any],
    ) -> Tuple[float, Optional[float]]:
        # Missing pricing is unknown, not a free API call. Ranking uses a
        # neutral prior; the displayed estimate remains null.
        if modality == "llm":
            if row.get("price_input_per_m") is None or row.get("price_output_per_m") is None:
                return 0.5, None
            in_price = float(row["price_input_per_m"])
            out_price = float(row["price_output_per_m"])
            est_tokens = max(1.0, prompt_size / 4.0)
            estimated = (in_price * est_tokens / 1_000_000.0) + (out_price * (est_tokens * 0.4) / 1_000_000.0)
        elif modality == "image":
            unit = row.get("price_unit") or {}
            if unit.get("per_image") is None:
                return 0.5, None
            estimated = float(unit["per_image"])
        elif modality == "video":
            unit = row.get("price_unit") or {}
            if unit.get("per_second") is None:
                return 0.5, None
            estimated = float(unit["per_second"]) * 4.0
        else:
            estimated = 0.0
        score = 1.0 / (1.0 + max(estimated, 0.0) * 10.0)
        return max(0.0, min(1.0, score)), round(float(estimated), 6)

    @staticmethod
    def _context_fit(required: int, available: Any) -> float:
        try:
            cap = int(available)
        except Exception:
            cap = 0
        if required <= 0:
            return 1.0 if cap > 0 else 0.75
        if cap <= 0:
            return 0.0
        if cap >= required:
            return 1.0
        return max(0.0, min(1.0, cap / float(required)))

    @staticmethod
    def _capability_fit(capabilities: Dict[str, Any], constraints: Dict[str, Any]) -> float:
        if not isinstance(capabilities, dict):
            capabilities = {}
        score = 1.0
        if constraints.get("require_json") and not bool(capabilities.get("json_mode")):
            return 0.0
        if constraints.get("vision_required") and not bool(capabilities.get("vision")):
            return 0.0
        if constraints.get("tool_calling") and not bool(capabilities.get("tool_calling")):
            score -= 0.3
        return max(0.0, min(1.0, score))

    @staticmethod
    def _local_fallback_candidate(modality: str, constraints: Dict[str, Any]) -> Dict[str, Any]:
        suffix = modality if modality in ("llm", "image", "video") else "parser"
        return {
            "registry_key": f"{modality}:local:deterministic",
            "modality": modality,
            "provider": "local",
            "model_id": f"deterministic-{suffix}",
            "display_name": f"Deterministic {suffix.title()}",
            "price_input_per_m": 0.0,
            "price_output_per_m": 0.0,
            "price_unit": {"per_image": 0.0, "per_second": 0.0},
            "capabilities": {"json_mode": True, "tool_calling": False},
            "policy_tags": ["internal_only", "no_pii"],
            "_constraints": constraints,
        }

    def _score_candidate(
        self,
        *,
        objective: str,
        task_class: str,
        modality: str,
        prompt_size: int,
        required_context: int,
        candidate: Dict[str, Any],
        recent_success_rate: float,
    ) -> Tuple[float, Dict[str, Any]]:
        quality = self._quality_proxy(candidate.get("benchmark") or {}, task_class)
        capability = self._capability_fit(candidate.get("capabilities") or {}, candidate.get("_constraints") or {})
        context_fit = self._context_fit(required_context, candidate.get("context_window"))
        latency = self._latency_score(candidate.get("median_latency_ms"))
        throughput = self._throughput_score(candidate.get("throughput_tps"))
        cost_score, estimated_cost = self._cost_score(
            modality=modality,
            prompt_size=prompt_size,
            row=candidate,
        )
        reliability = max(0.0, min(1.0, recent_success_rate))

        if objective == "performance":
            score = (
                0.50 * quality
                + 0.15 * capability
                + 0.10 * context_fit
                + 0.10 * reliability
                + 0.10 * throughput
                + 0.05 * latency
            )
        elif objective == "cost":
            score = (
                0.65 * cost_score
                + 0.20 * reliability
                + 0.10 * latency
                + 0.05 * context_fit
            )
        elif objective == "fastest":
            score = (
                0.55 * latency
                + 0.20 * throughput
                + 0.15 * reliability
                + 0.10 * capability
            )
        else:  # balanced + policy fallback scoring after filtering
            score = (
                0.35 * quality
                + 0.20 * cost_score
                + 0.20 * latency
                + 0.10 * throughput
                + 0.10 * reliability
                + 0.05 * capability
            )

        breakdown = {
            "quality_proxy": round(float(quality), 4),
            "capability_fit": round(float(capability), 4),
            "context_fit": round(float(context_fit), 4),
            "latency_score": round(float(latency), 4),
            "throughput_score": round(float(throughput), 4),
            "cost_score": round(float(cost_score), 4),
            "estimated_cost_usd": estimated_cost,
            "metric_defaults": [
                key for key in ("benchmark", "median_latency_ms", "throughput_tps")
                if not candidate.get(key)
            ] + (["pricing"] if estimated_cost is None else []) + ["recent_success_rate"],
            "recent_success_rate": round(float(reliability), 4),
            "final_score": round(float(score), 6),
        }
        return float(score), breakdown

    def plan_for_task(
        self,
        *,
        task_type: str,
        task_params: Dict[str, Any],
        workflow_routing: Dict[str, Any] | None = None,
        task_routing_override: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        modality = infer_modality(task_type)
        merged = _merge_routing(workflow_routing or {}, task_routing_override or {})
        objective = (merged.get("objective") or OBJECTIVE_DEFAULT).strip().lower()
        if objective not in OBJECTIVES:
            objective = OBJECTIVE_DEFAULT
        profile = (merged.get("policy_profile") or "default").strip().lower() or "default"
        constraints = dict(merged.get("constraints") or {})

        if bool(task_params.get("require_json")):
            constraints["require_json"] = True
        if task_params.get("max_latency_ms") not in (None, ""):
            try:
                constraints["max_latency_ms"] = int(float(task_params.get("max_latency_ms")))
            except Exception:
                pass

        explicit_provider = (task_params.get("explicit_provider") or task_params.get("provider") or "").strip().lower()
        requested_model = (task_params.get("model") or "").strip()
        prompt_text = ""
        for key in ("prompt", "text"):
            value = task_params.get(key)
            if isinstance(value, str) and value.strip():
                prompt_text += f" {value.strip()}"
        prompt_size = len(prompt_text.strip())

        candidates = self.store.list_registry_models(modality=modality, limit=1000)
        open_circuits = self.store.list_open_provider_circuits(modality=modality)
        filtered: List[Dict[str, Any]] = []
        for row in candidates:
            provider = (row.get("provider") or "").strip().lower()
            model_id = (row.get("model_id") or "").strip()
            if not provider or not model_id:
                continue
            if explicit_provider and provider != explicit_provider:
                continue
            if requested_model and model_id != requested_model:
                continue
            if not requested_model and "seed_pin_only" in (row.get("policy_tags") or []):
                continue
            if not self._provider_available(provider, modality):
                continue
            if constraints.get("no_external") and provider != "local":
                continue
            allowed = [str(item).strip().lower() for item in (constraints.get("allowed_providers") or []) if str(item).strip()]
            if allowed and provider not in allowed:
                continue
            blocked = {str(item).strip() for item in (constraints.get("blocked_models") or []) if str(item).strip()}
            if blocked and model_id in blocked:
                continue
            if constraints.get("max_latency_ms") not in (None, ""):
                try:
                    max_lat = float(constraints.get("max_latency_ms"))
                    latency = float(row.get("median_latency_ms") or 0.0)
                    if latency > 0 and max_lat > 0 and latency > max_lat:
                        continue
                except Exception:
                    pass
            if not self._policy_allowed(row.get("policy_tags") or [], profile):
                continue
            if provider in open_circuits:
                continue
            row_copy = dict(row)
            row_copy["_constraints"] = constraints
            filtered.append(row_copy)

        # Fallback to local deterministic candidate when no eligible registry
        # candidates remain after policy/availability filtering.
        if not filtered and not explicit_provider and not requested_model:
            filtered = [self._local_fallback_candidate(modality, constraints)]

        task_class = self._task_class(task_type, modality)
        scored: List[Dict[str, Any]] = []
        for row in filtered:
            provider = (row.get("provider") or "").strip().lower()
            score, breakdown = self._score_candidate(
                objective=objective,
                task_class=task_class,
                modality=modality,
                prompt_size=prompt_size,
                required_context=int(task_params.get("min_context_window") or 0) if str(task_params.get("min_context_window") or "").strip().isdigit() else 0,
                candidate=row,
                recent_success_rate=0.95 if provider == "local" else 0.9,
            )
            scored.append(
                {
                    **row,
                    "_score": score,
                    "_breakdown": breakdown,
                }
            )
        scored.sort(
            key=lambda item: (
                -float(item.get("_score") or 0.0),
                (item.get("_breakdown") or {}).get("estimated_cost_usd")
                if (item.get("_breakdown") or {}).get("estimated_cost_usd") is not None else float("inf"),
                float(item.get("median_latency_ms") or 999999.0),
                item.get("provider") or "",
                item.get("model_id") or "",
            )
        )

        selected = scored[0] if scored else {}
        fallback_chain = [
            {
                "provider": item.get("provider") or "",
                "model_id": item.get("model_id") or "",
                "score": round(float(item.get("_score") or 0.0), 6),
            }
            for item in scored[1:5]
        ]
        selected_payload = {
            "provider": selected.get("provider") or "",
            "model_id": selected.get("model_id") or "",
            "registry_key": selected.get("registry_key") or "",
            "score": round(float(selected.get("_score") or 0.0), 6),
        }
        summary = [
            {
                "provider": item.get("provider") or "",
                "model_id": item.get("model_id") or "",
                "score": round(float(item.get("_score") or 0.0), 6),
            }
            for item in scored[:8]
        ]
        return {
            "objective": objective,
            "modality": modality,
            "policy_profile": profile,
            "constraints": constraints,
            "candidate_count": len(scored),
            "selected": selected_payload,
            "fallback_chain": fallback_chain,
            "score_breakdown": selected.get("_breakdown") or {},
            "candidate_summary": summary,
        }

    def preview(
        self,
        *,
        task_type: str,
        task_params: Dict[str, Any] | None = None,
        routing: Dict[str, Any] | None = None,
        routing_override: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        return self.plan_for_task(
            task_type=task_type,
            task_params=task_params or {},
            workflow_routing=routing or {},
            task_routing_override=routing_override or {},
        )
