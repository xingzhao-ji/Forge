"""Runtime composition for Forge: store + executors + scheduler."""

from __future__ import annotations

import copy
import json
import os
from pathlib import Path
from typing import Any, Dict, Optional

from forge.local_tasks import ParseTextExtractExecutor, TextStatsExecutor
from forge.dispatcher import ForgeDispatcher
from forge.executors import ExecutionContext, ExecutorRegistry
from forge.registry import ForgeRegistry
from forge.routing import ForgeRoutingEngine
from forge.scheduler import ForgeScheduler
from forge.store import ForgeStore, WorkflowConflictError, WorkflowValidationError


class ForgeRuntime:
    def __init__(self, root_dir: str | Path):
        self.root_dir = str(Path(root_dir).expanduser().resolve())
        Path(self.root_dir).mkdir(parents=True, exist_ok=True)
        self.db_path = os.path.join(self.root_dir, "forge.sqlite3")
        self.templates_path = os.path.join(self.root_dir, "workflow_templates.json")
        self.registry_seed_path = os.path.join(self.root_dir, "registry_seed.json")
        self.store = ForgeStore(self.db_path)
        self.model_registry = ForgeRegistry(self.store, seed_path=self.registry_seed_path)
        self.model_registry.ensure_seeded()
        self.routing = ForgeRoutingEngine(self.store)
        self.registry = ExecutorRegistry()
        self.registry.register(ParseTextExtractExecutor())
        self.registry.register(TextStatsExecutor())
        self.context = ExecutionContext(root_dir=self.root_dir)
        self.dispatcher = ForgeDispatcher(self.store, self.registry, self.context, max_workers=4)
        self.scheduler = ForgeScheduler(self.store, self.dispatcher,
                                        attempt_planner=self._build_attempt_plan)
        self._started = False
        self._closed = False

    def start(self) -> None:
        if self._closed:
            raise RuntimeError("Create a new ForgeRuntime to reopen a stopped runtime.")
        if not self._started:
            self.scheduler.start()
            self._started = True

    def stop(self) -> None:
        if self._closed:
            return
        self.scheduler.stop(timeout=2.0)
        self._started = False
        self._closed = True

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.stop()

    def create_workflow(self, payload: Dict[str, Any], *, submitted_by: str = "local") -> Dict[str, Any]:
        expanded = self._expand_payload_from_template(payload)
        return self.store.create_workflow(
            expanded,
            supported_task_types=self.registry.task_types(),
            submitted_by=submitted_by,
        )

    def list_workflows(
        self,
        *,
        status: Optional[str] = None,
        task_type: Optional[str] = None,
        limit: int = 50,
    ):
        return self.store.list_workflows(status=status, task_type=task_type, limit=limit)

    def get_workflow_detail(self, workflow_id: str):
        return self.store.get_workflow_detail(workflow_id)

    def get_workflow_events(self, workflow_id: str, *, after_event_id: int = 0, limit: int = 200):
        return self.store.get_workflow_events(workflow_id, after_event_id=after_event_id, limit=limit)

    def cancel_workflow(self, workflow_id: str):
        return self.store.cancel_workflow(workflow_id)

    def resume_workflow(self, workflow_id: str):
        return self.store.resume_workflow(workflow_id)

    def _build_attempt_plan(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        workflow_input = payload.get("workflow_input") if isinstance(payload.get("workflow_input"), dict) else {}
        task_params = payload.get("task_params") if isinstance(payload.get("task_params"), dict) else {}
        task_routing = task_params.get("_routing") if isinstance(task_params.get("_routing"), dict) else {}
        workflow_routing = workflow_input.get("routing") if isinstance(workflow_input.get("routing"), dict) else {}
        return self.routing.plan_for_task(
            task_type=(payload.get("task_type") or ""),
            task_params=task_params,
            workflow_routing=workflow_routing,
            task_routing_override=task_routing,
        )

    def list_registry_models(self, *, modality: Optional[str] = None, provider: Optional[str] = None, limit: int = 500):
        return self.model_registry.list_models(modality=modality, provider=provider, limit=limit)

    def sync_registry(self, payload: Optional[Dict[str, Any]] = None):
        if payload and isinstance(payload, dict) and isinstance(payload.get("models"), list):
            return self.model_registry.sync_from_document(payload)
        return self.model_registry.sync_from_seed()

    def list_registry_sync_jobs(self, *, limit: int = 20):
        return self.store.list_sync_jobs(limit=limit)

    def preview_routing(
        self,
        *,
        task_type: str,
        task_params: Optional[Dict[str, Any]] = None,
        routing: Optional[Dict[str, Any]] = None,
        routing_override: Optional[Dict[str, Any]] = None,
    ):
        return self.routing.preview(
            task_type=task_type,
            task_params=task_params or {},
            routing=routing or {},
            routing_override=routing_override or {},
        )

    def _load_templates_document(self) -> Dict[str, Any]:
        if not os.path.exists(self.templates_path):
            return {"schema_version": 1, "templates": []}
        try:
            with open(self.templates_path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
        except Exception:
            return {"schema_version": 1, "templates": []}
        if not isinstance(data, dict):
            return {"schema_version": 1, "templates": []}
        schema_version = data.get("schema_version")
        try:
            schema_version = int(schema_version)
        except Exception:
            schema_version = 1
        templates = data.get("templates")
        if not isinstance(templates, list):
            templates = []
        return {"schema_version": max(1, schema_version), "templates": templates}

    def templates_schema_version(self) -> int:
        doc = self._load_templates_document()
        return int(doc.get("schema_version") or 1)

    @staticmethod
    def _normalize_template(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not isinstance(raw, dict):
            return None
        template_id = (raw.get("template_id") or raw.get("id") or "").strip()
        if not template_id:
            return None
        try:
            version = int(raw.get("version") or 1)
        except Exception:
            version = 1
        version = max(1, version)
        stable_id = (raw.get("id") or f"{template_id}@v{version}").strip() or f"{template_id}@v{version}"
        normalized = {
            "id": stable_id,
            "template_id": template_id,
            "version": version,
            "name": (raw.get("name") or template_id).strip() or template_id,
            "description": (raw.get("description") or "").strip(),
            "inputs": raw.get("inputs") if isinstance(raw.get("inputs"), dict) else {},
            "nodes": raw.get("nodes") if isinstance(raw.get("nodes"), list) else [],
        }
        if isinstance(raw.get("routing"), dict):
            normalized["routing"] = raw.get("routing")
        if raw.get("budget_usd") not in (None, ""):
            normalized["budget_usd"] = raw.get("budget_usd")
        return normalized

    def list_templates(
        self,
        *,
        template_id: Optional[str] = None,
        version: Optional[int] = None,
        latest_only: bool = True,
    ):
        doc = self._load_templates_document()
        normalized = []
        for item in doc.get("templates") or []:
            record = self._normalize_template(item)
            if record:
                normalized.append(record)
        normalized.sort(key=lambda item: (item["template_id"], -int(item["version"]), item["id"]))

        if template_id:
            tid = template_id.strip()
            normalized = [
                item for item in normalized
                if item["template_id"] == tid or item["id"] == tid
            ]

        if version is not None:
            try:
                target_version = int(version)
            except Exception:
                target_version = None
            if target_version is not None:
                normalized = [item for item in normalized if int(item["version"]) == target_version]

        if latest_only:
            latest: Dict[str, Dict[str, Any]] = {}
            for item in normalized:
                key = item["template_id"]
                if key not in latest or int(item["version"]) > int(latest[key]["version"]):
                    latest[key] = item
            normalized = sorted(latest.values(), key=lambda item: item["name"].lower())
        return normalized

    def resolve_template(self, template_id: str, *, version: Optional[int] = None) -> Optional[Dict[str, Any]]:
        if not template_id:
            return None
        templates = self.list_templates(
            template_id=template_id,
            version=version,
            latest_only=(version is None),
        )
        if not templates:
            return None
        return copy.deepcopy(templates[0])

    @staticmethod
    def _apply_inputs_to_nodes(nodes: list[Dict[str, Any]], inputs: Dict[str, Any]) -> None:
        note_id = str(inputs.get("note_id") or "").strip()
        prompt = str(inputs.get("prompt") or "").strip()
        seeded_prompt = False
        for node in nodes:
            if not isinstance(node, dict):
                continue
            task_type = (node.get("type") or "").strip()
            params = node.get("params")
            if not isinstance(params, dict):
                params = {}
            if note_id and task_type in ("notes.extract", "parse.pdf.extract"):
                params["note_id"] = note_id
            depends_on = node.get("depends_on")
            if not isinstance(depends_on, list):
                depends_on = []
            if prompt and task_type == "llm.summarize" and not seeded_prompt and len(depends_on) == 0:
                params["text"] = prompt
                seeded_prompt = True
            if prompt and task_type in ("image.generate", "video.generate"):
                params["prompt"] = prompt
            node["params"] = params

    def _expand_payload_from_template(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(payload, dict):
            return payload
        if isinstance(payload.get("nodes"), list) and payload.get("nodes"):
            return payload

        template_ref = (
            (payload.get("template_id") or "").strip()
            or (payload.get("template") or "").strip()
            or (payload.get("template_name") or "").strip()
        )
        if not template_ref:
            return payload

        template_version = payload.get("template_version")
        resolved = self.resolve_template(template_ref, version=template_version)
        if not resolved:
            raise WorkflowValidationError(f"Template not found: {template_ref}")

        merged_inputs = dict(resolved.get("inputs") or {})
        if isinstance(payload.get("inputs"), dict):
            merged_inputs.update(payload["inputs"])
        if payload.get("note_id") not in (None, ""):
            merged_inputs["note_id"] = payload.get("note_id")
        if payload.get("prompt") not in (None, ""):
            merged_inputs["prompt"] = payload.get("prompt")

        nodes = copy.deepcopy(resolved.get("nodes") or [])
        self._apply_inputs_to_nodes(nodes, merged_inputs)
        expanded = {
            "name": (payload.get("name") or resolved.get("name") or resolved.get("template_id") or "workflow"),
            "idempotency_key": payload.get("idempotency_key"),
            "inputs": merged_inputs,
            "nodes": nodes,
        }
        template_routing = resolved.get("routing") if isinstance(resolved.get("routing"), dict) else {}
        payload_routing = payload.get("routing") if isinstance(payload.get("routing"), dict) else {}
        if template_routing or payload_routing:
            merged_routing = dict(template_routing)
            if payload_routing.get("objective"):
                merged_routing["objective"] = payload_routing.get("objective")
            template_constraints = template_routing.get("constraints") if isinstance(template_routing.get("constraints"), dict) else {}
            payload_constraints = payload_routing.get("constraints") if isinstance(payload_routing.get("constraints"), dict) else {}
            merged_constraints = dict(template_constraints)
            merged_constraints.update(payload_constraints)
            if merged_constraints:
                merged_routing["constraints"] = merged_constraints
            expanded["routing"] = merged_routing
        if payload.get("budget_usd") not in (None, ""):
            expanded["budget_usd"] = payload.get("budget_usd")
        elif resolved.get("budget_usd") not in (None, ""):
            expanded["budget_usd"] = resolved.get("budget_usd")
        return expanded


__all__ = [
    "ForgeRuntime",
    "WorkflowValidationError",
    "WorkflowConflictError",
]
