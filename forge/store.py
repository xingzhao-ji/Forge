"""Durable SQLite store for Forge runtime."""

from __future__ import annotations

import json
import os
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from forge.models import (
    TASK_STATUS_BLOCKED,
    TASK_STATUS_CANCELLED,
    TASK_STATUS_FAILED,
    TASK_STATUS_QUEUED,
    TASK_STATUS_RETRY_WAIT,
    TASK_STATUS_RUNNING,
    TASK_STATUS_SUCCEEDED,
    WORKFLOW_STATUS_CANCELLED,
    WORKFLOW_STATUS_CANCEL_REQUESTED,
    WORKFLOW_STATUS_COMPLETED,
    WORKFLOW_STATUS_FAILED,
    WORKFLOW_STATUS_QUEUED,
    WORKFLOW_STATUS_RUNNING,
    artifact_id,
    attempt_id,
    canonical_json,
    iso_utc,
    next_backoff_seconds,
    now_utc,
    parse_iso,
    payload_hash,
    plus_seconds,
    task_id,
    workflow_id,
)


class WorkflowValidationError(ValueError):
    pass


class WorkflowConflictError(ValueError):
    pass


@dataclass
class ClaimLimits:
    llm_max: int = 4
    image_max: int = 2
    video_max: int = 1
    default_max: int = 4


def _blob_id() -> str:
    return f"blb_{uuid.uuid4().hex[:12]}"


def _task_group(task_type: str) -> str:
    if task_type.startswith("llm."):
        return "llm"
    if task_type.startswith("image.generate"):
        return "image"
    if task_type.startswith("video.generate"):
        return "video"
    return "default"


def _task_modality(task_type: str) -> str:
    if task_type.startswith("llm."):
        return "llm"
    if task_type.startswith("image.generate"):
        return "image"
    if task_type.startswith("video.generate"):
        return "video"
    if task_type.startswith("parse.") or task_type == "notes.extract":
        return "parse"
    return "default"


ROUTING_OBJECTIVES = {"balanced", "performance", "cost", "fastest", "policy"}
OBJECTIVE_PROVIDER_POLICY = {
    "balanced": "latency_first",
    "performance": "quality_first",
    "cost": "cost_first",
    "fastest": "latency_first",
    "policy": "latency_first",
}
ROUTING_MAX_COST_USD = 10_000.0
ROUTING_MAX_LATENCY_MS = 600_000


class ForgeStore:
    def __init__(
        self,
        db_path: str,
        *,
        max_nodes: int = 100,
        max_payload_bytes: int = 1_000_000,
        priority_aging_seconds: int = 30,
    ):
        self.db_path = db_path
        self.max_nodes = max_nodes
        self.max_payload_bytes = max_payload_bytes
        self.priority_aging_seconds = max(5, int(priority_aging_seconds))
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.create_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        return conn

    @staticmethod
    def _loads(value: Any, default: Any = None) -> Any:
        if value in (None, ""):
            return default
        try:
            return json.loads(value)
        except Exception:
            return default

    @staticmethod
    def _dumps(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))

    @staticmethod
    def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        return {str(row["name"]) for row in rows}

    def _ensure_column(
        self,
        conn: sqlite3.Connection,
        table: str,
        column: str,
        declaration: str,
    ) -> None:
        columns = self._table_columns(conn, table)
        if column in columns:
            return
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {declaration}")

    def create_schema(self) -> None:
        conn = self._connect()
        try:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS workflows (
                    workflow_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    idempotency_key TEXT UNIQUE,
                    input_hash TEXT NOT NULL,
                    submitted_by TEXT,
                    submitted_at TEXT NOT NULL,
                    started_at TEXT,
                    completed_at TEXT,
                    input_json TEXT NOT NULL,
                    budget_usd REAL,
                    spent_usd REAL NOT NULL DEFAULT 0,
                    summary_json TEXT,
                    error_json TEXT
                );

                CREATE TABLE IF NOT EXISTS tasks (
                    task_id TEXT PRIMARY KEY,
                    workflow_id TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    task_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    priority INTEGER NOT NULL DEFAULT 100,
                    max_attempts INTEGER NOT NULL DEFAULT 3,
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    next_run_at TEXT,
                    lease_owner TEXT,
                    lease_expires_at TEXT,
                    input_json TEXT NOT NULL,
                    output_json TEXT,
                    last_error_json TEXT,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    FOREIGN KEY(workflow_id) REFERENCES workflows(workflow_id)
                );

                CREATE UNIQUE INDEX IF NOT EXISTS idx_tasks_workflow_node
                  ON tasks(workflow_id, node_id);
                CREATE INDEX IF NOT EXISTS idx_tasks_sched
                  ON tasks(status, next_run_at, lease_expires_at);

                CREATE TABLE IF NOT EXISTS task_dependencies (
                    task_id TEXT NOT NULL,
                    depends_on_task_id TEXT NOT NULL,
                    PRIMARY KEY(task_id, depends_on_task_id),
                    FOREIGN KEY(task_id) REFERENCES tasks(task_id),
                    FOREIGN KEY(depends_on_task_id) REFERENCES tasks(task_id)
                );

                CREATE TABLE IF NOT EXISTS task_attempts (
                    attempt_id TEXT PRIMARY KEY,
                    task_id TEXT NOT NULL,
                    attempt_no INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    ended_at TEXT,
                    error_code TEXT,
                    error_json TEXT,
                    log_excerpt TEXT,
                    provider_used TEXT,
                    model_used TEXT,
                    objective TEXT,
                    candidate_count INTEGER,
                    fallback_chain_json TEXT,
                    score_breakdown_json TEXT,
                    attempt_plan_json TEXT,
                    fallback_used INTEGER NOT NULL DEFAULT 0,
                    provider_job_id TEXT,
                    cost_usd REAL NOT NULL DEFAULT 0,
                    duration_ms INTEGER,
                    FOREIGN KEY(task_id) REFERENCES tasks(task_id)
                );

                CREATE TABLE IF NOT EXISTS artifacts (
                    artifact_id TEXT PRIMARY KEY,
                    workflow_id TEXT NOT NULL,
                    task_id TEXT NOT NULL,
                    artifact_type TEXT NOT NULL,
                    path_or_uri TEXT NOT NULL,
                    sha256 TEXT,
                    bytes INTEGER,
                    metadata_json TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(workflow_id) REFERENCES workflows(workflow_id),
                    FOREIGN KEY(task_id) REFERENCES tasks(task_id)
                );

                CREATE TABLE IF NOT EXISTS workflow_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    workflow_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    payload_json TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(workflow_id) REFERENCES workflows(workflow_id)
                );

                CREATE INDEX IF NOT EXISTS idx_workflow_events_cursor
                  ON workflow_events(workflow_id, event_id);

                CREATE TABLE IF NOT EXISTS artifact_blobs (
                    blob_id TEXT PRIMARY KEY,
                    sha256 TEXT NOT NULL UNIQUE,
                    bytes INTEGER NOT NULL DEFAULT 0,
                    canonical_path TEXT NOT NULL,
                    ref_count INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_artifact_blobs_sha
                  ON artifact_blobs(sha256);

                CREATE TABLE IF NOT EXISTS provider_health (
                    provider_key TEXT NOT NULL,
                    modality TEXT NOT NULL,
                    consecutive_failures INTEGER NOT NULL DEFAULT 0,
                    circuit_state TEXT NOT NULL DEFAULT 'closed',
                    reopen_at TEXT,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(provider_key, modality)
                );
                CREATE INDEX IF NOT EXISTS idx_provider_health_state
                  ON provider_health(modality, circuit_state, reopen_at);

                CREATE TABLE IF NOT EXISTS model_registry (
                    registry_key TEXT PRIMARY KEY,
                    modality TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    model_id TEXT NOT NULL,
                    display_name TEXT,
                    price_input_per_m REAL,
                    price_output_per_m REAL,
                    price_unit_json TEXT,
                    median_latency_ms REAL,
                    throughput_tps REAL,
                    context_window INTEGER,
                    benchmark_json TEXT,
                    capabilities_json TEXT,
                    policy_tags_json TEXT,
                    source_name TEXT,
                    source_ref TEXT,
                    synced_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_model_registry_modality
                  ON model_registry(modality, provider);

                CREATE TABLE IF NOT EXISTS sync_jobs (
                    job_id TEXT PRIMARY KEY,
                    source_name TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    status TEXT NOT NULL,
                    checksum TEXT,
                    details_json TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_sync_jobs_status
                  ON sync_jobs(status, started_at);
                """
            )
            self._ensure_column(conn, "workflows", "budget_usd", "REAL")
            self._ensure_column(conn, "workflows", "spent_usd", "REAL NOT NULL DEFAULT 0")
            self._ensure_column(conn, "workflows", "objective", "TEXT")
            self._ensure_column(conn, "task_attempts", "cost_usd", "REAL NOT NULL DEFAULT 0")
            self._ensure_column(conn, "task_attempts", "model_used", "TEXT")
            self._ensure_column(conn, "task_attempts", "objective", "TEXT")
            self._ensure_column(conn, "task_attempts", "candidate_count", "INTEGER")
            self._ensure_column(conn, "task_attempts", "fallback_chain_json", "TEXT")
            self._ensure_column(conn, "task_attempts", "score_breakdown_json", "TEXT")
            self._ensure_column(conn, "task_attempts", "attempt_plan_json", "TEXT")
            self._ensure_column(conn, "task_attempts", "fallback_used", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(conn, "task_attempts", "provider_job_id", "TEXT")
            conn.execute(
                """
                UPDATE workflows
                SET spent_usd = 0
                WHERE spent_usd IS NULL
                """
            )
            conn.execute(
                """
                UPDATE task_attempts
                SET fallback_used = 0
                WHERE fallback_used IS NULL
                """
            )
            conn.commit()
        finally:
            conn.close()

    def _append_event_tx(
        self,
        conn: sqlite3.Connection,
        workflow_id_value: str,
        event_type: str,
        payload: Dict[str, Any] | None = None,
        created_at: Optional[str] = None,
    ) -> None:
        conn.execute(
            """
            INSERT INTO workflow_events (workflow_id, event_type, payload_json, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (
                workflow_id_value,
                event_type,
                self._dumps(payload or {}),
                created_at or iso_utc(),
            ),
        )

    @staticmethod
    def _policy_from_objective(objective: str) -> str:
        return OBJECTIVE_PROVIDER_POLICY.get((objective or "").strip().lower(), "")

    def _normalize_routing(
        self,
        raw: Any,
        *,
        location: str,
    ) -> Dict[str, Any]:
        if raw in (None, ""):
            return {}
        if not isinstance(raw, dict):
            raise WorkflowValidationError(f"{location} must be an object.")

        objective = (raw.get("objective") or "").strip().lower()
        if objective and objective not in ROUTING_OBJECTIVES:
            allowed = ", ".join(sorted(ROUTING_OBJECTIVES))
            raise WorkflowValidationError(f"{location}.objective must be one of: {allowed}.")

        policy_profile = (raw.get("policy_profile") or "").strip().lower()
        prefer_cached_registry = raw.get("prefer_cached_registry")

        constraints_raw = raw.get("constraints")
        constraints: Dict[str, Any] = {}
        if constraints_raw not in (None, ""):
            if not isinstance(constraints_raw, dict):
                raise WorkflowValidationError(f"{location}.constraints must be an object.")

            if constraints_raw.get("max_cost_usd") not in (None, ""):
                try:
                    max_cost = float(constraints_raw.get("max_cost_usd"))
                except Exception as exc:
                    raise WorkflowValidationError(f"{location}.constraints.max_cost_usd must be numeric.") from exc
                if max_cost < 0:
                    raise WorkflowValidationError(f"{location}.constraints.max_cost_usd must be >= 0.")
                if max_cost > ROUTING_MAX_COST_USD:
                    raise WorkflowValidationError(
                        f"{location}.constraints.max_cost_usd exceeds cap ({ROUTING_MAX_COST_USD})."
                    )
                constraints["max_cost_usd"] = round(max_cost, 4)

            if constraints_raw.get("max_latency_ms") not in (None, ""):
                try:
                    max_latency = int(float(constraints_raw.get("max_latency_ms")))
                except Exception as exc:
                    raise WorkflowValidationError(f"{location}.constraints.max_latency_ms must be numeric.") from exc
                if max_latency < 0:
                    raise WorkflowValidationError(f"{location}.constraints.max_latency_ms must be >= 0.")
                if max_latency > ROUTING_MAX_LATENCY_MS:
                    raise WorkflowValidationError(
                        f"{location}.constraints.max_latency_ms exceeds cap ({ROUTING_MAX_LATENCY_MS})."
                    )
                constraints["max_latency_ms"] = max_latency

            policy_value = (constraints_raw.get("policy") or "").strip()
            if policy_value:
                constraints["policy"] = policy_value

            allowed = constraints_raw.get("allowed_providers")
            if allowed not in (None, ""):
                if not isinstance(allowed, list):
                    raise WorkflowValidationError(f"{location}.constraints.allowed_providers must be an array.")
                cleaned = [str(item).strip().lower() for item in allowed if str(item).strip()]
                if cleaned:
                    constraints["allowed_providers"] = cleaned

            blocked = constraints_raw.get("blocked_models")
            if blocked not in (None, ""):
                if not isinstance(blocked, list):
                    raise WorkflowValidationError(f"{location}.constraints.blocked_models must be an array.")
                cleaned = [str(item).strip() for item in blocked if str(item).strip()]
                if cleaned:
                    constraints["blocked_models"] = cleaned

            data_sensitivity = (constraints_raw.get("data_sensitivity") or "").strip()
            if data_sensitivity:
                constraints["data_sensitivity"] = data_sensitivity

            region = (constraints_raw.get("region") or "").strip()
            if region:
                constraints["region"] = region

            if "no_external" in constraints_raw:
                constraints["no_external"] = bool(constraints_raw.get("no_external"))

            if "require_json" in constraints_raw:
                constraints["require_json"] = bool(constraints_raw.get("require_json"))

        # v3 shorthand keys (routing_override at node level often uses these directly).
        if raw.get("max_latency_ms") not in (None, "") and "max_latency_ms" not in constraints:
            try:
                max_latency = int(float(raw.get("max_latency_ms")))
            except Exception as exc:
                raise WorkflowValidationError(f"{location}.max_latency_ms must be numeric.") from exc
            if max_latency < 0:
                raise WorkflowValidationError(f"{location}.max_latency_ms must be >= 0.")
            if max_latency > ROUTING_MAX_LATENCY_MS:
                raise WorkflowValidationError(
                    f"{location}.max_latency_ms exceeds cap ({ROUTING_MAX_LATENCY_MS})."
                )
            constraints["max_latency_ms"] = max_latency

        if raw.get("max_total_cost_usd") not in (None, ""):
            try:
                max_cost_total = float(raw.get("max_total_cost_usd"))
            except Exception as exc:
                raise WorkflowValidationError(f"{location}.max_total_cost_usd must be numeric.") from exc
            if max_cost_total < 0:
                raise WorkflowValidationError(f"{location}.max_total_cost_usd must be >= 0.")
            if max_cost_total > ROUTING_MAX_COST_USD:
                raise WorkflowValidationError(
                    f"{location}.max_total_cost_usd exceeds cap ({ROUTING_MAX_COST_USD})."
                )
            constraints.setdefault("max_cost_usd", round(max_cost_total, 4))

        if "require_json" in raw and "require_json" not in constraints:
            constraints["require_json"] = bool(raw.get("require_json"))

        normalized: Dict[str, Any] = {}
        if objective:
            normalized["objective"] = objective
        if policy_profile:
            normalized["policy_profile"] = policy_profile
        if prefer_cached_registry is not None:
            normalized["prefer_cached_registry"] = bool(prefer_cached_registry)
        if constraints:
            normalized["constraints"] = constraints
            if "max_cost_usd" in constraints:
                normalized["max_total_cost_usd"] = constraints["max_cost_usd"]
        return normalized

    @staticmethod
    def _merge_routing(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        base_obj = base if isinstance(base, dict) else {}
        override_obj = override if isinstance(override, dict) else {}
        if not base_obj and not override_obj:
            return {}

        merged: Dict[str, Any] = {}
        objective = (
            (override_obj.get("objective") or "").strip().lower()
            or (base_obj.get("objective") or "").strip().lower()
        )
        if objective:
            merged["objective"] = objective
        policy_profile = (
            (override_obj.get("policy_profile") or "").strip().lower()
            or (base_obj.get("policy_profile") or "").strip().lower()
        )
        if policy_profile:
            merged["policy_profile"] = policy_profile
        if "prefer_cached_registry" in override_obj:
            merged["prefer_cached_registry"] = bool(override_obj.get("prefer_cached_registry"))
        elif "prefer_cached_registry" in base_obj:
            merged["prefer_cached_registry"] = bool(base_obj.get("prefer_cached_registry"))

        constraints: Dict[str, Any] = {}
        if isinstance(base_obj.get("constraints"), dict):
            constraints.update(base_obj.get("constraints") or {})
        if isinstance(override_obj.get("constraints"), dict):
            constraints.update(override_obj.get("constraints") or {})
        if constraints:
            merged["constraints"] = constraints
            if "max_cost_usd" in constraints:
                merged["max_total_cost_usd"] = constraints["max_cost_usd"]
        return merged

    def validate_submission(
        self,
        submission: Dict[str, Any],
        *,
        supported_task_types: Iterable[str],
    ) -> Dict[str, Any]:
        if not isinstance(submission, dict):
            raise WorkflowValidationError("Payload must be an object.")

        name = (submission.get("name") or "").strip()
        if not name:
            raise WorkflowValidationError("Workflow name is required.")

        nodes = submission.get("nodes")
        if not isinstance(nodes, list) or not nodes:
            raise WorkflowValidationError("Workflow nodes are required.")
        if len(nodes) > self.max_nodes:
            raise WorkflowValidationError(f"Workflow exceeds node limit ({self.max_nodes}).")
        workflow_routing = self._normalize_routing(
            submission.get("routing"),
            location="routing",
        )

        supported = set(supported_task_types or [])
        node_ids = set()
        normalized_nodes = []
        for raw in nodes:
            if not isinstance(raw, dict):
                raise WorkflowValidationError("Each node must be an object.")
            node_id_value = (raw.get("id") or "").strip()
            task_type = (raw.get("type") or "").strip()
            depends_on = raw.get("depends_on") or []
            params = dict(raw.get("params") or {})
            max_attempts = raw.get("max_attempts", 3)
            priority = raw.get("priority", 100)

            if not node_id_value:
                raise WorkflowValidationError("Node id is required.")
            if node_id_value in node_ids:
                raise WorkflowValidationError(f"Duplicate node id: {node_id_value}")
            node_ids.add(node_id_value)

            if not task_type:
                raise WorkflowValidationError(f"Node {node_id_value} missing type.")
            if task_type not in supported:
                raise WorkflowValidationError(f"Unsupported task type: {task_type}")

            if not isinstance(depends_on, list):
                raise WorkflowValidationError(f"Node {node_id_value} depends_on must be an array.")
            if not isinstance(params, dict):
                raise WorkflowValidationError(f"Node {node_id_value} params must be an object.")
            try:
                max_attempts = int(max_attempts)
                priority = int(priority)
            except Exception as exc:
                raise WorkflowValidationError(
                    f"Node {node_id_value} max_attempts/priority must be integers."
                ) from exc
            if max_attempts < 1:
                raise WorkflowValidationError(f"Node {node_id_value} max_attempts must be >= 1.")

            node_override_raw = raw.get("routing_override")
            if node_override_raw in (None, ""):
                node_override_raw = params.get("routing_override")
            node_routing = self._normalize_routing(
                node_override_raw,
                location=f"Node {node_id_value} routing_override",
            )
            effective_routing = self._merge_routing(workflow_routing, node_routing)
            if node_routing and workflow_routing:
                workflow_constraints = workflow_routing.get("constraints") or {}
                node_constraints = node_routing.get("constraints") or {}
                allowed_workflow = set(workflow_constraints.get("allowed_providers") or [])
                allowed_node = set(node_constraints.get("allowed_providers") or [])
                if allowed_workflow and allowed_node and not allowed_node.issubset(allowed_workflow):
                    raise WorkflowValidationError(
                        f"Node {node_id_value} routing_override.allowed_providers may only narrow workflow policy."
                    )
                blocked_workflow = set(str(x) for x in (workflow_constraints.get("blocked_models") or []))
                blocked_node = set(str(x) for x in (node_constraints.get("blocked_models") or []))
                if blocked_workflow and blocked_node and not blocked_workflow.issubset(blocked_node):
                    raise WorkflowValidationError(
                        f"Node {node_id_value} routing_override.blocked_models may not remove workflow deny rules."
                    )
            if effective_routing:
                params["_routing"] = effective_routing
                if not (params.get("provider_policy") or "").strip():
                    mapped_policy = self._policy_from_objective(effective_routing.get("objective") or "")
                    if mapped_policy:
                        params["provider_policy"] = mapped_policy

            normalized_nodes.append(
                {
                    "id": node_id_value,
                    "type": task_type,
                    "depends_on": [str(item).strip() for item in depends_on],
                    "params": params,
                    "max_attempts": max_attempts,
                    "priority": priority,
                }
            )

        node_lookup = {node["id"]: node for node in normalized_nodes}
        for node in normalized_nodes:
            for dep in node["depends_on"]:
                if dep not in node_lookup:
                    raise WorkflowValidationError(
                        f"Node {node['id']} depends_on unknown node: {dep}"
                    )

        # Cycle detection (DFS)
        marks: Dict[str, int] = {node["id"]: 0 for node in normalized_nodes}  # 0 new, 1 temp, 2 done

        def visit(node_id_value: str) -> None:
            state = marks[node_id_value]
            if state == 1:
                raise WorkflowValidationError("Workflow DAG contains a cycle.")
            if state == 2:
                return
            marks[node_id_value] = 1
            for dep in node_lookup[node_id_value]["depends_on"]:
                visit(dep)
            marks[node_id_value] = 2

        for node in normalized_nodes:
            visit(node["id"])

        budget_raw = submission.get("budget_usd")
        budget_usd = None
        routing_budget = None
        if workflow_routing:
            routing_budget = (
                workflow_routing.get("max_total_cost_usd")
                if workflow_routing.get("max_total_cost_usd") not in (None, "")
                else (workflow_routing.get("constraints") or {}).get("max_cost_usd")
            )
        if budget_raw in (None, "") and routing_budget not in (None, ""):
            budget_raw = routing_budget
        if budget_raw not in (None, ""):
            try:
                budget_usd = float(budget_raw)
            except Exception as exc:
                raise WorkflowValidationError("budget_usd must be a number.") from exc
            if budget_usd <= 0:
                raise WorkflowValidationError("budget_usd must be greater than 0.")

        normalized = {
            "name": name,
            "idempotency_key": (submission.get("idempotency_key") or "").strip() or None,
            "inputs": submission.get("inputs") if isinstance(submission.get("inputs"), dict) else {},
            "budget_usd": budget_usd,
            "nodes": normalized_nodes,
        }
        if workflow_routing:
            normalized["routing"] = workflow_routing
        body = canonical_json(normalized)
        if len(body.encode("utf-8")) > self.max_payload_bytes:
            raise WorkflowValidationError("Workflow payload exceeds size limit.")
        return normalized

    def create_workflow(
        self,
        submission: Dict[str, Any],
        *,
        supported_task_types: Iterable[str],
        submitted_by: str = "local",
    ) -> Dict[str, Any]:
        normalized = self.validate_submission(
            submission,
            supported_task_types=supported_task_types,
        )
        now = iso_utc()
        idem = normalized.get("idempotency_key")
        input_json = self._dumps(normalized)
        input_hash = payload_hash(normalized)
        budget_usd = normalized.get("budget_usd")

        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            if idem:
                existing = conn.execute(
                    """
                    SELECT workflow_id, input_hash, status, submitted_at, budget_usd, spent_usd, objective
                    FROM workflows
                    WHERE idempotency_key = ?
                    """,
                    (idem,),
                ).fetchone()
                if existing:
                    if existing["input_hash"] != input_hash:
                        raise WorkflowConflictError(
                            "Idempotency key already exists with different payload."
                        )
                    conn.commit()
                    return {
                        "workflow_id": existing["workflow_id"],
                        "status": existing["status"],
                        "task_count": conn.execute(
                            "SELECT COUNT(*) AS c FROM tasks WHERE workflow_id = ?",
                            (existing["workflow_id"],),
                        ).fetchone()["c"],
                        "accepted_at": existing["submitted_at"],
                        "budget_usd": existing["budget_usd"],
                        "spent_usd": float(existing["spent_usd"] or 0),
                        "objective": (existing["objective"] or "balanced"),
                        "created": False,
                    }

            wf_id = workflow_id()
            node_to_task: Dict[str, str] = {node["id"]: task_id() for node in normalized["nodes"]}
            objective = ((normalized.get("routing") or {}).get("objective") or "balanced").strip().lower()
            conn.execute(
                """
                INSERT INTO workflows (
                    workflow_id, name, status, idempotency_key, input_hash,
                    submitted_by, submitted_at, input_json, budget_usd, spent_usd, objective
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
                """,
                (
                    wf_id,
                    normalized["name"],
                    WORKFLOW_STATUS_QUEUED,
                    idem,
                    input_hash,
                    submitted_by,
                    now,
                    input_json,
                    budget_usd,
                    objective,
                ),
            )
            for node in normalized["nodes"]:
                t_id = node_to_task[node["id"]]
                conn.execute(
                    """
                    INSERT INTO tasks (
                        task_id, workflow_id, node_id, task_type, status,
                        priority, max_attempts, attempt_count, next_run_at,
                        input_json, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)
                    """,
                    (
                        t_id,
                        wf_id,
                        node["id"],
                        node["type"],
                        TASK_STATUS_QUEUED,
                        node["priority"],
                        node["max_attempts"],
                        now,
                        self._dumps(node.get("params") or {}),
                        now,
                    ),
                )
            for node in normalized["nodes"]:
                for dep in node["depends_on"]:
                    conn.execute(
                        """
                        INSERT INTO task_dependencies (task_id, depends_on_task_id)
                        VALUES (?, ?)
                        """,
                        (node_to_task[node["id"]], node_to_task[dep]),
                    )
            self._append_event_tx(
                conn,
                wf_id,
                "workflow.submitted",
                {
                    "name": normalized["name"],
                    "task_count": len(normalized["nodes"]),
                    "idempotency_key": idem,
                    "routing": normalized.get("routing") or {},
                },
                created_at=now,
            )
            conn.commit()
            return {
                "workflow_id": wf_id,
                "status": WORKFLOW_STATUS_QUEUED,
                "task_count": len(normalized["nodes"]),
                "accepted_at": now,
                "budget_usd": budget_usd,
                "spent_usd": 0.0,
                "objective": objective,
                "created": True,
            }
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def list_workflows(
        self,
        *,
        status: Optional[str] = None,
        task_type: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        limit = max(1, min(200, int(limit)))
        conn = self._connect()
        try:
            args: List[Any] = []
            query = """
                SELECT workflow_id, name, status, submitted_by, submitted_at, started_at, completed_at,
                       budget_usd, spent_usd, objective
                FROM workflows
            """
            clauses = []
            if status:
                clauses.append("status = ?")
                args.append(status)
            if task_type:
                clauses.append(
                    """
                    workflow_id IN (
                        SELECT workflow_id FROM tasks WHERE task_type = ?
                    )
                    """
                )
                args.append(task_type)
            if clauses:
                query += " WHERE " + " AND ".join(clauses)
            query += " ORDER BY submitted_at DESC LIMIT ?"
            args.append(limit)
            rows = conn.execute(query, tuple(args)).fetchall()
            out = []
            for row in rows:
                counters = conn.execute(
                    """
                    SELECT status, COUNT(*) AS c
                    FROM tasks
                    WHERE workflow_id = ?
                    GROUP BY status
                    """,
                    (row["workflow_id"],),
                ).fetchall()
                status_counts = {item["status"]: item["c"] for item in counters}
                out.append(
                    {
                        "workflow_id": row["workflow_id"],
                        "name": row["name"],
                        "status": row["status"],
                        "submitted_by": row["submitted_by"] or "",
                        "submitted_at": row["submitted_at"],
                        "started_at": row["started_at"],
                        "completed_at": row["completed_at"],
                        "budget_usd": row["budget_usd"],
                        "spent_usd": float(row["spent_usd"] or 0),
                        "objective": (row["objective"] or "balanced"),
                        "status_counts": status_counts,
                    }
                )
            return out
        finally:
            conn.close()

    def get_workflow(self, workflow_id_value: str) -> Optional[Dict[str, Any]]:
        conn = self._connect()
        try:
            row = conn.execute(
                """
                SELECT *
                FROM workflows
                WHERE workflow_id = ?
                """,
                (workflow_id_value,),
            ).fetchone()
            if not row:
                return None
            return {
                "workflow_id": row["workflow_id"],
                "name": row["name"],
                "status": row["status"],
                "idempotency_key": row["idempotency_key"],
                "submitted_by": row["submitted_by"],
                "submitted_at": row["submitted_at"],
                "started_at": row["started_at"],
                "completed_at": row["completed_at"],
                "budget_usd": row["budget_usd"],
                "spent_usd": float(row["spent_usd"] or 0),
                "objective": (row["objective"] or "balanced"),
                "input": self._loads(row["input_json"], {}),
                "summary": self._loads(row["summary_json"], {}),
                "error": self._loads(row["error_json"], {}),
            }
        finally:
            conn.close()

    def get_workflow_events(
        self,
        workflow_id_value: str,
        *,
        after_event_id: int = 0,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        limit = max(1, min(1000, int(limit)))
        conn = self._connect()
        try:
            rows = conn.execute(
                """
                SELECT event_id, event_type, payload_json, created_at
                FROM workflow_events
                WHERE workflow_id = ? AND event_id > ?
                ORDER BY event_id ASC
                LIMIT ?
                """,
                (workflow_id_value, int(after_event_id), limit),
            ).fetchall()
            return [
                {
                    "event_id": row["event_id"],
                    "event_type": row["event_type"],
                    "payload": self._loads(row["payload_json"], {}),
                    "created_at": row["created_at"],
                }
                for row in rows
            ]
        finally:
            conn.close()

    def get_workflow_detail(self, workflow_id_value: str) -> Optional[Dict[str, Any]]:
        conn = self._connect()
        try:
            workflow = conn.execute(
                "SELECT * FROM workflows WHERE workflow_id = ?",
                (workflow_id_value,),
            ).fetchone()
            if not workflow:
                return None
            task_rows = conn.execute(
                """
                SELECT *
                FROM tasks
                WHERE workflow_id = ?
                ORDER BY created_at ASC, node_id ASC
                """,
                (workflow_id_value,),
            ).fetchall()
            tasks: List[Dict[str, Any]] = []
            task_id_to_node = {}
            for row in task_rows:
                task_id_to_node[row["task_id"]] = row["node_id"]
            deps_rows = conn.execute(
                """
                SELECT task_id, depends_on_task_id
                FROM task_dependencies
                WHERE task_id IN (
                    SELECT task_id FROM tasks WHERE workflow_id = ?
                )
                """,
                (workflow_id_value,),
            ).fetchall()
            deps_by_task: Dict[str, List[str]] = {}
            for dep_row in deps_rows:
                deps_by_task.setdefault(dep_row["task_id"], []).append(dep_row["depends_on_task_id"])

            latest_attempts = conn.execute(
                """
                SELECT a.task_id, a.status, a.log_excerpt, a.error_code, a.error_json,
                       a.provider_used, a.model_used, a.objective, a.candidate_count,
                       a.fallback_chain_json, a.score_breakdown_json, a.fallback_used, a.provider_job_id,
                       a.started_at, a.ended_at, a.cost_usd
                FROM task_attempts a
                JOIN (
                    SELECT task_id, MAX(attempt_no) AS max_attempt
                    FROM task_attempts
                    GROUP BY task_id
                ) m ON m.task_id = a.task_id AND m.max_attempt = a.attempt_no
                WHERE a.task_id IN (
                    SELECT task_id FROM tasks WHERE workflow_id = ?
                )
                """,
                (workflow_id_value,),
            ).fetchall()
            latest_attempt_by_task = {row["task_id"]: row for row in latest_attempts}
            attempt_rows = conn.execute(
                """
                SELECT task_id, attempt_no, status, started_at, ended_at,
                       provider_used, model_used, objective, candidate_count,
                       fallback_chain_json, score_breakdown_json, fallback_used, provider_job_id,
                       error_code, error_json, log_excerpt, cost_usd
                FROM task_attempts
                WHERE task_id IN (
                    SELECT task_id FROM tasks WHERE workflow_id = ?
                )
                ORDER BY task_id ASC, attempt_no ASC
                """,
                (workflow_id_value,),
            ).fetchall()
            attempts_by_task: Dict[str, List[Dict[str, Any]]] = {}
            for row in attempt_rows:
                attempts_by_task.setdefault(row["task_id"], []).append(
                    {
                        "attempt_no": int(row["attempt_no"] or 0),
                        "status": row["status"] or "",
                        "started_at": row["started_at"] or "",
                        "ended_at": row["ended_at"] or "",
                        "provider": row["provider_used"] or "",
                        "model": row["model_used"] or "",
                        "objective": row["objective"] or "",
                        "candidate_count": int(row["candidate_count"] or 0),
                        "fallback_chain": self._loads(row["fallback_chain_json"], []) or [],
                        "score_breakdown": self._loads(row["score_breakdown_json"], {}) or {},
                        "fallback_used": bool(row["fallback_used"]),
                        "provider_job_id": row["provider_job_id"] or "",
                        "error_code": row["error_code"] or "",
                        "error": self._loads(row["error_json"], {}) or {},
                        "log_excerpt": row["log_excerpt"] or "",
                        "cost_usd": float(row["cost_usd"] or 0.0),
                    }
                )

            for row in task_rows:
                deps_task_ids = deps_by_task.get(row["task_id"], [])
                deps_node_ids = [task_id_to_node.get(dep_id, dep_id) for dep_id in deps_task_ids]
                attempt = latest_attempt_by_task.get(row["task_id"])
                tasks.append(
                    {
                        "task_id": row["task_id"],
                        "node_id": row["node_id"],
                        "task_type": row["task_type"],
                        "status": row["status"],
                        "priority": row["priority"],
                        "attempt_count": row["attempt_count"],
                        "max_attempts": row["max_attempts"],
                        "next_run_at": row["next_run_at"],
                        "lease_owner": row["lease_owner"],
                        "lease_expires_at": row["lease_expires_at"],
                        "started_at": row["started_at"],
                        "finished_at": row["finished_at"],
                        "params": self._loads(row["input_json"], {}),
                        "outputs": self._loads(row["output_json"], {}),
                        "last_error": self._loads(row["last_error_json"], {}),
                        "depends_on_task_ids": deps_task_ids,
                        "depends_on_node_ids": deps_node_ids,
                        "latest_attempt": {
                            "status": attempt["status"] if attempt else "",
                            "log_excerpt": attempt["log_excerpt"] if attempt else "",
                            "error_code": attempt["error_code"] if attempt else "",
                            "error": self._loads(attempt["error_json"], {}) if attempt else {},
                            "provider_used": attempt["provider_used"] if attempt else "",
                            "model_used": attempt["model_used"] if attempt else "",
                            "objective": attempt["objective"] if attempt else "",
                            "candidate_count": int(attempt["candidate_count"] or 0) if attempt else 0,
                            "fallback_chain": self._loads(attempt["fallback_chain_json"], []) if attempt else [],
                            "score_breakdown": self._loads(attempt["score_breakdown_json"], {}) if attempt else {},
                            "fallback_used": bool(attempt["fallback_used"]) if attempt else False,
                            "provider_job_id": attempt["provider_job_id"] if attempt else "",
                            "started_at": attempt["started_at"] if attempt else "",
                            "ended_at": attempt["ended_at"] if attempt else "",
                            "cost_usd": float(attempt["cost_usd"] or 0) if attempt else 0.0,
                        },
                        "attempts": attempts_by_task.get(row["task_id"], []),
                    }
                )

            artifacts_rows = conn.execute(
                """
                SELECT artifact_id, task_id, artifact_type, path_or_uri, sha256, bytes, metadata_json, created_at
                FROM artifacts
                WHERE workflow_id = ?
                ORDER BY created_at ASC
                """,
                (workflow_id_value,),
            ).fetchall()
            artifacts = [
                {
                    "artifact_id": row["artifact_id"],
                    "task_id": row["task_id"],
                    "artifact_type": row["artifact_type"],
                    "path_or_uri": row["path_or_uri"],
                    "sha256": row["sha256"] or "",
                    "bytes": row["bytes"] or 0,
                    "metadata": self._loads(row["metadata_json"], {}),
                    "created_at": row["created_at"],
                }
                for row in artifacts_rows
            ]
            events = self.get_workflow_events(workflow_id_value, after_event_id=0, limit=500)
            return {
                "workflow": {
                    "workflow_id": workflow["workflow_id"],
                    "name": workflow["name"],
                    "status": workflow["status"],
                    "idempotency_key": workflow["idempotency_key"],
                    "submitted_by": workflow["submitted_by"],
                    "submitted_at": workflow["submitted_at"],
                    "started_at": workflow["started_at"],
                    "completed_at": workflow["completed_at"],
                    "budget_usd": workflow["budget_usd"],
                    "spent_usd": float(workflow["spent_usd"] or 0),
                    "objective": workflow["objective"] or "balanced",
                    "input": self._loads(workflow["input_json"], {}),
                    "summary": self._loads(workflow["summary_json"], {}),
                    "error": self._loads(workflow["error_json"], {}),
                },
                "tasks": tasks,
                "artifacts": artifacts,
                "events": events,
            }
        finally:
            conn.close()

    def _dependencies_succeeded_tx(self, conn: sqlite3.Connection, task_id_value: str) -> bool:
        row = conn.execute(
            """
            SELECT COUNT(*) AS unsatisfied
            FROM task_dependencies d
            JOIN tasks t ON t.task_id = d.depends_on_task_id
            WHERE d.task_id = ?
              AND t.status != ?
            """,
            (task_id_value, TASK_STATUS_SUCCEEDED),
        ).fetchone()
        return int(row["unsatisfied"]) == 0

    def _running_counts_tx(self, conn: sqlite3.Connection) -> Dict[str, int]:
        rows = conn.execute(
            """
            SELECT task_type, COUNT(*) AS c
            FROM tasks
            WHERE status = ?
            GROUP BY task_type
            """,
            (TASK_STATUS_RUNNING,),
        ).fetchall()
        out = {"llm": 0, "image": 0, "video": 0, "default": 0}
        for row in rows:
            group = _task_group(row["task_type"])
            out[group] = out.get(group, 0) + int(row["c"])
        return out

    def _effective_priority(self, row: sqlite3.Row, now_iso: str) -> int:
        base = int(row["priority"] or 100)
        now_dt = parse_iso(now_iso) or now_utc()
        created_dt = parse_iso(row["created_at"]) or now_dt
        waited_seconds = max(0, int((now_dt - created_dt).total_seconds()))
        aging_bonus = waited_seconds // self.priority_aging_seconds
        return max(0, base - aging_bonus)

    def estimate_task_cost(self, task_type: str, params: Dict[str, Any] | None = None) -> float:
        cfg = params or {}
        prompt_text = ""
        if isinstance(cfg.get("prompt"), str):
            prompt_text += cfg["prompt"]
        if isinstance(cfg.get("text"), str):
            prompt_text += " " + cfg["text"]
        prompt_chars = len(prompt_text.strip())

        if task_type.startswith("llm."):
            # Heuristic token/cost approximation for guardrails.
            return round(0.001 + (prompt_chars / 1000.0) * 0.0015, 6)
        if task_type.startswith("image.generate"):
            return 0.04
        if task_type.startswith("video.generate"):
            return 0.60
        return 0.0

    def _mark_budget_exceeded_tx(
        self,
        conn: sqlite3.Connection,
        row: sqlite3.Row,
        *,
        now_iso: str,
        estimated_cost: float,
    ) -> None:
        budget = float(row["budget_usd"] or 0)
        spent = float(row["spent_usd"] or 0)
        error_payload = {
            "code": "budget_exceeded",
            "message": "Workflow budget would be exceeded by claiming this task.",
            "estimated_cost_usd": estimated_cost,
            "spent_usd": spent,
            "budget_usd": budget,
        }
        conn.execute(
            """
            UPDATE tasks
            SET status = ?,
                next_run_at = NULL,
                lease_owner = NULL,
                lease_expires_at = NULL,
                finished_at = ?,
                last_error_json = ?
            WHERE task_id = ?
              AND status IN (?, ?)
            """,
            (
                TASK_STATUS_FAILED,
                now_iso,
                self._dumps(error_payload),
                row["task_id"],
                TASK_STATUS_QUEUED,
                TASK_STATUS_RETRY_WAIT,
            ),
        )
        self._mark_blocked_tasks_tx(conn, row["workflow_id"], now_iso)
        conn.execute(
            """
            UPDATE workflows
            SET status = ?,
                completed_at = COALESCE(completed_at, ?),
                error_json = ?
            WHERE workflow_id = ?
              AND status NOT IN (?, ?, ?)
            """,
            (
                WORKFLOW_STATUS_FAILED,
                now_iso,
                self._dumps(error_payload),
                row["workflow_id"],
                WORKFLOW_STATUS_COMPLETED,
                WORKFLOW_STATUS_FAILED,
                WORKFLOW_STATUS_CANCELLED,
            ),
        )
        self._append_event_tx(
            conn,
            row["workflow_id"],
            "task.failed",
            {
                "task_id": row["task_id"],
                "node_id": row["node_id"],
                "error": error_payload,
            },
            created_at=now_iso,
        )
        self._append_event_tx(
            conn,
            row["workflow_id"],
            "workflow.failed",
            {"reason": "budget_exceeded", "error": error_payload},
            created_at=now_iso,
        )

    def _add_workflow_spend_tx(
        self,
        conn: sqlite3.Connection,
        workflow_id_value: str,
        cost_usd: float,
    ) -> None:
        if cost_usd <= 0:
            return
        conn.execute(
            """
            UPDATE workflows
            SET spent_usd = COALESCE(spent_usd, 0) + ?
            WHERE workflow_id = ?
            """,
            (float(cost_usd), workflow_id_value),
        )

    def _upsert_artifact_blob_tx(
        self,
        conn: sqlite3.Connection,
        *,
        sha256: str,
        bytes_count: int,
        path_or_uri: str,
        now_iso: str,
    ) -> Dict[str, Any]:
        if not sha256:
            return {}
        row = conn.execute(
            """
            SELECT blob_id, canonical_path, ref_count
            FROM artifact_blobs
            WHERE sha256 = ?
            """,
            (sha256,),
        ).fetchone()
        if row:
            conn.execute(
                """
                UPDATE artifact_blobs
                SET ref_count = ref_count + 1,
                    updated_at = ?
                WHERE blob_id = ?
                """,
                (now_iso, row["blob_id"]),
            )
            return {
                "blob_id": row["blob_id"],
                "canonical_path": row["canonical_path"] or path_or_uri,
                "ref_count": int(row["ref_count"] or 0) + 1,
                "deduped": True,
            }

        blob = _blob_id()
        conn.execute(
            """
            INSERT INTO artifact_blobs (
                blob_id, sha256, bytes, canonical_path, ref_count, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, 1, ?, ?)
            """,
            (blob, sha256, int(bytes_count), path_or_uri, now_iso, now_iso),
        )
        return {
            "blob_id": blob,
            "canonical_path": path_or_uri,
            "ref_count": 1,
            "deduped": False,
        }

    def claim_runnable_tasks(
        self,
        worker_id: str,
        *,
        now: Optional[str] = None,
        limits: Optional[ClaimLimits] = None,
        lease_ttl_seconds: int = 30,
        max_tasks: int = 8,
        attempt_planner: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
    ) -> List[Dict[str, Any]]:
        claim_limits = limits or ClaimLimits()
        now_iso = now or iso_utc()
        lease_exp = iso_utc(plus_seconds(now_utc(), lease_ttl_seconds))
        conn = self._connect()
        claimed: List[Dict[str, Any]] = []
        try:
            conn.execute("BEGIN IMMEDIATE")
            candidates = conn.execute(
                """
                SELECT t.*, w.budget_usd, w.spent_usd, w.input_json AS workflow_input_json
                FROM tasks t
                JOIN workflows w ON w.workflow_id = t.workflow_id
                WHERE t.status IN (?, ?)
                  AND (t.next_run_at IS NULL OR t.next_run_at <= ?)
                  AND w.status IN (?, ?)
                ORDER BY t.created_at ASC
                LIMIT 200
                """,
                (
                    TASK_STATUS_QUEUED,
                    TASK_STATUS_RETRY_WAIT,
                    now_iso,
                    WORKFLOW_STATUS_QUEUED,
                    WORKFLOW_STATUS_RUNNING,
                ),
            ).fetchall()
            running = self._running_counts_tx(conn)
            candidates_sorted = sorted(
                candidates,
                key=lambda row: (
                    self._effective_priority(row, now_iso),
                    int(row["priority"] or 100),
                    row["created_at"],
                    row["task_id"],
                ),
            )

            for row in candidates_sorted:
                if len(claimed) >= max_tasks:
                    break
                if not self._dependencies_succeeded_tx(conn, row["task_id"]):
                    continue

                params = self._loads(row["input_json"], {}) or {}
                estimated_cost = self.estimate_task_cost(row["task_type"], params=params)
                budget = float(row["budget_usd"] or 0)
                spent = float(row["spent_usd"] or 0)
                if budget > 0 and (spent + estimated_cost) > budget:
                    self._mark_budget_exceeded_tx(
                        conn,
                        row,
                        now_iso=now_iso,
                        estimated_cost=estimated_cost,
                    )
                    continue

                group = _task_group(row["task_type"])
                cap = {
                    "llm": claim_limits.llm_max,
                    "image": claim_limits.image_max,
                    "video": claim_limits.video_max,
                    "default": claim_limits.default_max,
                }.get(group, claim_limits.default_max)
                if running.get(group, 0) >= cap:
                    continue

                next_attempt = int(row["attempt_count"]) + 1
                task_params = self._loads(row["input_json"], {}) or {}
                attempt_plan = {}
                if attempt_planner is not None:
                    try:
                        attempt_plan = attempt_planner(
                            {
                                "workflow_id": row["workflow_id"],
                                "task_id": row["task_id"],
                                "task_type": row["task_type"],
                                "node_id": row["node_id"],
                                "attempt_no": next_attempt,
                                "workflow_input": self._loads(row["workflow_input_json"], {}),
                                "task_params": task_params,
                            }
                        ) or {}
                    except Exception:
                        attempt_plan = {}

                selected = attempt_plan.get("selected") if isinstance(attempt_plan, dict) else {}
                fallback_chain = attempt_plan.get("fallback_chain") if isinstance(attempt_plan, dict) else []
                score_breakdown = attempt_plan.get("score_breakdown") if isinstance(attempt_plan, dict) else {}
                provider_selected = ""
                model_selected = ""
                objective_selected = ""
                candidate_count = 0
                if isinstance(selected, dict):
                    provider_selected = (selected.get("provider") or "").strip().lower()
                    model_selected = (selected.get("model_id") or selected.get("model") or "").strip()
                if isinstance(attempt_plan, dict):
                    objective_selected = (attempt_plan.get("objective") or "").strip().lower()
                    try:
                        candidate_count = int(attempt_plan.get("candidate_count") or 0)
                    except Exception:
                        candidate_count = 0

                updated = conn.execute(
                    """
                    UPDATE tasks
                    SET status = ?,
                        lease_owner = ?,
                        lease_expires_at = ?,
                        attempt_count = ?,
                        started_at = COALESCE(started_at, ?)
                    WHERE task_id = ?
                      AND status IN (?, ?)
                    """,
                    (
                        TASK_STATUS_RUNNING,
                        worker_id,
                        lease_exp,
                        next_attempt,
                        now_iso,
                        row["task_id"],
                        TASK_STATUS_QUEUED,
                        TASK_STATUS_RETRY_WAIT,
                    ),
                )
                if updated.rowcount != 1:
                    continue

                conn.execute(
                    """
                    INSERT INTO task_attempts (
                        attempt_id, task_id, attempt_no, status, started_at,
                        provider_used, model_used, objective, candidate_count,
                        fallback_chain_json, score_breakdown_json, attempt_plan_json, fallback_used
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                    """,
                    (
                        attempt_id(),
                        row["task_id"],
                        next_attempt,
                        TASK_STATUS_RUNNING,
                        now_iso,
                        provider_selected or None,
                        model_selected or None,
                        objective_selected or None,
                        candidate_count if candidate_count > 0 else None,
                        self._dumps(fallback_chain or []),
                        self._dumps(score_breakdown or {}),
                        self._dumps(attempt_plan or {}),
                    ),
                )
                conn.execute(
                    """
                    UPDATE workflows
                    SET status = ?,
                        started_at = COALESCE(started_at, ?)
                    WHERE workflow_id = ?
                      AND status = ?
                    """,
                    (WORKFLOW_STATUS_RUNNING, now_iso, row["workflow_id"], WORKFLOW_STATUS_QUEUED),
                )
                self._append_event_tx(
                    conn,
                    row["workflow_id"],
                    "task.started",
                    {
                        "task_id": row["task_id"],
                        "node_id": row["node_id"],
                        "task_type": row["task_type"],
                        "attempt_no": next_attempt,
                        "objective": objective_selected or "",
                        "provider": provider_selected or "",
                        "model": model_selected or "",
                    },
                    created_at=now_iso,
                )
                claimed.append(
                    {
                        "task_id": row["task_id"],
                        "workflow_id": row["workflow_id"],
                        "node_id": row["node_id"],
                        "task_type": row["task_type"],
                        "attempt_count": next_attempt,
                        "max_attempts": row["max_attempts"],
                        "attempt_plan": attempt_plan or {},
                    }
                )
                running[group] = running.get(group, 0) + 1

            conn.commit()
            return claimed
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def load_task_for_execution(self, task_id_value: str) -> Optional[Dict[str, Any]]:
        conn = self._connect()
        try:
            task = conn.execute(
                "SELECT * FROM tasks WHERE task_id = ?",
                (task_id_value,),
            ).fetchone()
            if not task:
                return None
            dep_rows = conn.execute(
                """
                SELECT d.depends_on_task_id, t.node_id, t.task_type, t.status, t.output_json
                FROM task_dependencies d
                JOIN tasks t ON t.task_id = d.depends_on_task_id
                WHERE d.task_id = ?
                """,
                (task_id_value,),
            ).fetchall()
            upstream = {}
            for row in dep_rows:
                upstream[row["node_id"]] = {
                    "task_id": row["depends_on_task_id"],
                    "task_type": row["task_type"],
                    "status": row["status"],
                    **(self._loads(row["output_json"], {}) or {}),
                }
            attempt = conn.execute(
                """
                SELECT provider_used, model_used, objective, attempt_plan_json
                FROM task_attempts
                WHERE task_id = ?
                ORDER BY attempt_no DESC
                LIMIT 1
                """,
                (task_id_value,),
            ).fetchone()
            return {
                "task_id": task["task_id"],
                "workflow_id": task["workflow_id"],
                "node_id": task["node_id"],
                "task_type": task["task_type"],
                "status": task["status"],
                "attempt_count": task["attempt_count"],
                "max_attempts": task["max_attempts"],
                "params": self._loads(task["input_json"], {}) or {},
                "upstream": upstream,
                "attempt_plan": self._loads(attempt["attempt_plan_json"], {}) if attempt else {},
                "attempt_provider": (attempt["provider_used"] if attempt else "") or "",
                "attempt_model": (attempt["model_used"] if attempt else "") or "",
                "attempt_objective": (attempt["objective"] if attempt else "") or "",
            }
        finally:
            conn.close()

    def _close_attempt_tx(
        self,
        conn: sqlite3.Connection,
        task_id_value: str,
        *,
        workflow_id_value: str,
        status: str,
        ended_at: str,
        error_code: str = "",
        error_json: Optional[Dict[str, Any]] = None,
        log_excerpt: str = "",
        provider_used: str = "",
        model_used: str = "",
        objective: str = "",
        score_breakdown: Optional[Dict[str, Any]] = None,
        fallback_used: bool = False,
        provider_job_id: str = "",
        cost_usd: float = 0.0,
    ) -> None:
        attempt_row = conn.execute(
            """
            SELECT attempt_id, started_at
            FROM task_attempts
            WHERE task_id = ?
            ORDER BY attempt_no DESC
            LIMIT 1
            """,
            (task_id_value,),
        ).fetchone()
        if not attempt_row:
            return
        duration_ms = None
        started = attempt_row["started_at"]
        try:
            start_dt = datetime.fromisoformat(started)
            end_dt = datetime.fromisoformat(ended_at)
            duration_ms = int((end_dt - start_dt).total_seconds() * 1000)
        except Exception:
            duration_ms = None
        conn.execute(
            """
            UPDATE task_attempts
            SET status = ?,
                ended_at = ?,
                error_code = ?,
                error_json = ?,
                log_excerpt = ?,
                provider_used = ?,
                model_used = COALESCE(?, model_used),
                objective = COALESCE(?, objective),
                score_breakdown_json = COALESCE(?, score_breakdown_json),
                fallback_used = CASE WHEN ? THEN 1 ELSE fallback_used END,
                provider_job_id = COALESCE(?, provider_job_id),
                cost_usd = ?,
                duration_ms = ?
            WHERE attempt_id = ?
            """,
            (
                status,
                ended_at,
                error_code or None,
                self._dumps(error_json or {}) if error_json else None,
                log_excerpt or None,
                provider_used or None,
                model_used or None,
                objective or None,
                self._dumps(score_breakdown or {}) if score_breakdown else None,
                bool(fallback_used),
                provider_job_id or None,
                float(cost_usd or 0.0),
                duration_ms,
                attempt_row["attempt_id"],
            ),
        )
        self._add_workflow_spend_tx(conn, workflow_id_value, float(cost_usd or 0.0))

    def _mark_blocked_tasks_tx(self, conn: sqlite3.Connection, workflow_id_value: str, now_iso: str) -> int:
        total_blocked = 0
        while True:
            rows = conn.execute(
                """
                SELECT DISTINCT t.task_id
                FROM tasks t
                JOIN task_dependencies d ON d.task_id = t.task_id
                JOIN tasks dep ON dep.task_id = d.depends_on_task_id
                WHERE t.workflow_id = ?
                  AND t.status IN (?, ?)
                  AND dep.status IN (?, ?, ?)
                """,
                (
                    workflow_id_value,
                    TASK_STATUS_QUEUED,
                    TASK_STATUS_RETRY_WAIT,
                    TASK_STATUS_FAILED,
                    TASK_STATUS_BLOCKED,
                    TASK_STATUS_CANCELLED,
                ),
            ).fetchall()
            if not rows:
                break
            for row in rows:
                conn.execute(
                    """
                    UPDATE tasks
                    SET status = ?, finished_at = ?, next_run_at = NULL
                    WHERE task_id = ?
                      AND status IN (?, ?)
                    """,
                    (
                        TASK_STATUS_BLOCKED,
                        now_iso,
                        row["task_id"],
                        TASK_STATUS_QUEUED,
                        TASK_STATUS_RETRY_WAIT,
                    ),
                )
                total_blocked += 1
        return total_blocked

    def complete_task(
        self,
        task_id_value: str,
        *,
        outputs: Optional[Dict[str, Any]] = None,
        artifacts: Optional[List[Dict[str, Any]]] = None,
        logs: Optional[List[str]] = None,
        provider_used: str = "local",
        model_used: str = "",
        objective: str = "",
        score_breakdown: Optional[Dict[str, Any]] = None,
        fallback_used: bool = False,
        provider_job_id: str = "",
        cost_usd: float = 0.0,
    ) -> None:
        now_iso = iso_utc()
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT workflow_id, node_id FROM tasks WHERE task_id = ?",
                (task_id_value,),
            ).fetchone()
            if not row:
                conn.rollback()
                return
            output_json = self._dumps(outputs or {})
            log_excerpt = "\n".join(logs or [])[:2000]
            conn.execute(
                """
                UPDATE tasks
                SET status = ?,
                    output_json = ?,
                    lease_owner = NULL,
                    lease_expires_at = NULL,
                    next_run_at = NULL,
                    finished_at = ?,
                    last_error_json = NULL
                WHERE task_id = ?
                """,
                (TASK_STATUS_SUCCEEDED, output_json, now_iso, task_id_value),
            )
            self._close_attempt_tx(
                conn,
                task_id_value,
                workflow_id_value=row["workflow_id"],
                status=TASK_STATUS_SUCCEEDED,
                ended_at=now_iso,
                log_excerpt=log_excerpt,
                provider_used=provider_used,
                model_used=model_used,
                objective=objective,
                score_breakdown=score_breakdown,
                fallback_used=fallback_used,
                provider_job_id=provider_job_id,
                cost_usd=cost_usd,
            )
            for artifact in artifacts or []:
                metadata = dict(artifact.get("metadata") or {})
                blob = self._upsert_artifact_blob_tx(
                    conn,
                    sha256=(artifact.get("sha256") or "").strip(),
                    bytes_count=int(artifact.get("bytes") or 0),
                    path_or_uri=artifact.get("path_or_uri") or "",
                    now_iso=now_iso,
                )
                if blob:
                    metadata["blob_id"] = blob["blob_id"]
                    metadata["blob_ref_count"] = blob["ref_count"]
                    metadata["canonical_path"] = blob["canonical_path"]
                    metadata["deduped"] = bool(blob["deduped"])
                conn.execute(
                    """
                    INSERT INTO artifacts (
                        artifact_id, workflow_id, task_id, artifact_type,
                        path_or_uri, sha256, bytes, metadata_json, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        artifact_id(),
                        row["workflow_id"],
                        task_id_value,
                        artifact.get("artifact_type") or "file",
                        artifact.get("path_or_uri") or "",
                        artifact.get("sha256") or "",
                        int(artifact.get("bytes") or 0),
                        self._dumps(metadata),
                        now_iso,
                    ),
                )
            self._append_event_tx(
                conn,
                row["workflow_id"],
                "task.succeeded",
                {"task_id": task_id_value, "node_id": row["node_id"]},
                created_at=now_iso,
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        self.refresh_workflow_state(row["workflow_id"])

    def retry_task(
        self,
        task_id_value: str,
        *,
        error: Dict[str, Any],
        next_run_at: Optional[str] = None,
        logs: Optional[List[str]] = None,
        provider_used: str = "",
        model_used: str = "",
        objective: str = "",
        score_breakdown: Optional[Dict[str, Any]] = None,
        fallback_used: bool = False,
        provider_job_id: str = "",
        cost_usd: float = 0.0,
    ) -> None:
        now_iso = iso_utc()
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT workflow_id, node_id, attempt_count FROM tasks WHERE task_id = ?",
                (task_id_value,),
            ).fetchone()
            if not row:
                conn.rollback()
                return
            if not next_run_at:
                wait = next_backoff_seconds(int(row["attempt_count"]))
                next_run_at = iso_utc(now_utc() + timedelta(seconds=wait))
            conn.execute(
                """
                UPDATE tasks
                SET status = ?,
                    next_run_at = ?,
                    lease_owner = NULL,
                    lease_expires_at = NULL,
                    last_error_json = ?
                WHERE task_id = ?
                """,
                (TASK_STATUS_RETRY_WAIT, next_run_at, self._dumps(error), task_id_value),
            )
            self._close_attempt_tx(
                conn,
                task_id_value,
                workflow_id_value=row["workflow_id"],
                status=TASK_STATUS_RETRY_WAIT,
                ended_at=now_iso,
                error_code=error.get("code") or "retry_wait",
                error_json=error,
                log_excerpt="\n".join(logs or [])[:2000],
                provider_used=provider_used,
                model_used=model_used,
                objective=objective,
                score_breakdown=score_breakdown,
                fallback_used=fallback_used,
                provider_job_id=provider_job_id,
                cost_usd=cost_usd,
            )
            self._append_event_tx(
                conn,
                row["workflow_id"],
                "task.retry_scheduled",
                {
                    "task_id": task_id_value,
                    "node_id": row["node_id"],
                    "next_run_at": next_run_at,
                },
                created_at=now_iso,
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        self.refresh_workflow_state(row["workflow_id"])

    def fail_task(
        self,
        task_id_value: str,
        *,
        error: Dict[str, Any],
        logs: Optional[List[str]] = None,
        provider_used: str = "",
        model_used: str = "",
        objective: str = "",
        score_breakdown: Optional[Dict[str, Any]] = None,
        fallback_used: bool = False,
        provider_job_id: str = "",
        cost_usd: float = 0.0,
    ) -> None:
        now_iso = iso_utc()
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT workflow_id, node_id FROM tasks WHERE task_id = ?",
                (task_id_value,),
            ).fetchone()
            if not row:
                conn.rollback()
                return
            conn.execute(
                """
                UPDATE tasks
                SET status = ?,
                    lease_owner = NULL,
                    lease_expires_at = NULL,
                    next_run_at = NULL,
                    finished_at = ?,
                    last_error_json = ?
                WHERE task_id = ?
                """,
                (TASK_STATUS_FAILED, now_iso, self._dumps(error), task_id_value),
            )
            self._close_attempt_tx(
                conn,
                task_id_value,
                workflow_id_value=row["workflow_id"],
                status=TASK_STATUS_FAILED,
                ended_at=now_iso,
                error_code=error.get("code") or "task_failed",
                error_json=error,
                log_excerpt="\n".join(logs or [])[:2000],
                provider_used=provider_used,
                model_used=model_used,
                objective=objective,
                score_breakdown=score_breakdown,
                fallback_used=fallback_used,
                provider_job_id=provider_job_id,
                cost_usd=cost_usd,
            )
            self._mark_blocked_tasks_tx(conn, row["workflow_id"], now_iso)
            self._append_event_tx(
                conn,
                row["workflow_id"],
                "task.failed",
                {"task_id": task_id_value, "node_id": row["node_id"], "error": error},
                created_at=now_iso,
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        self.refresh_workflow_state(row["workflow_id"])

    def cancel_task(
        self,
        task_id_value: str,
        *,
        reason: str = "cancelled",
        provider_used: str = "",
        model_used: str = "",
        objective: str = "",
        score_breakdown: Optional[Dict[str, Any]] = None,
        fallback_used: bool = False,
        provider_job_id: str = "",
        cost_usd: float = 0.0,
    ) -> None:
        now_iso = iso_utc()
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT workflow_id, node_id FROM tasks WHERE task_id = ?",
                (task_id_value,),
            ).fetchone()
            if not row:
                conn.rollback()
                return
            conn.execute(
                """
                UPDATE tasks
                SET status = ?,
                    lease_owner = NULL,
                    lease_expires_at = NULL,
                    next_run_at = NULL,
                    finished_at = ?
                WHERE task_id = ?
                """,
                (TASK_STATUS_CANCELLED, now_iso, task_id_value),
            )
            self._close_attempt_tx(
                conn,
                task_id_value,
                workflow_id_value=row["workflow_id"],
                status=TASK_STATUS_CANCELLED,
                ended_at=now_iso,
                error_code="cancelled",
                error_json={"code": "cancelled", "message": reason},
                provider_used=provider_used,
                model_used=model_used,
                objective=objective,
                score_breakdown=score_breakdown,
                fallback_used=fallback_used,
                provider_job_id=provider_job_id,
                cost_usd=cost_usd,
            )
            self._append_event_tx(
                conn,
                row["workflow_id"],
                "task.cancelled",
                {"task_id": task_id_value, "node_id": row["node_id"], "reason": reason},
                created_at=now_iso,
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        self.refresh_workflow_state(row["workflow_id"])

    def recover_expired_leases(self, *, now: Optional[str] = None) -> int:
        now_iso = now or iso_utc()
        conn = self._connect()
        recovered = 0
        touched_workflows = set()
        try:
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute(
                """
                SELECT task_id, workflow_id, node_id, attempt_count, max_attempts
                FROM tasks
                WHERE status = ?
                  AND lease_expires_at IS NOT NULL
                  AND lease_expires_at <= ?
                """,
                (TASK_STATUS_RUNNING, now_iso),
            ).fetchall()
            for row in rows:
                touched_workflows.add(row["workflow_id"])
                error_payload = {"code": "lease_expired", "message": "Task lease expired."}
                if int(row["attempt_count"]) < int(row["max_attempts"]):
                    wait = next_backoff_seconds(int(row["attempt_count"]))
                    next_run = iso_utc(now_utc() + timedelta(seconds=wait))
                    conn.execute(
                        """
                        UPDATE tasks
                        SET status = ?,
                            next_run_at = ?,
                            lease_owner = NULL,
                            lease_expires_at = NULL,
                            last_error_json = ?
                        WHERE task_id = ?
                        """,
                        (TASK_STATUS_RETRY_WAIT, next_run, self._dumps(error_payload), row["task_id"]),
                    )
                    self._close_attempt_tx(
                        conn,
                        row["task_id"],
                        workflow_id_value=row["workflow_id"],
                        status="abandoned",
                        ended_at=now_iso,
                        error_code="lease_expired",
                        error_json=error_payload,
                    )
                    self._append_event_tx(
                        conn,
                        row["workflow_id"],
                        "task.abandoned",
                        {"task_id": row["task_id"], "node_id": row["node_id"], "next_run_at": next_run},
                        created_at=now_iso,
                    )
                else:
                    conn.execute(
                        """
                        UPDATE tasks
                        SET status = ?,
                            next_run_at = NULL,
                            lease_owner = NULL,
                            lease_expires_at = NULL,
                            finished_at = ?,
                            last_error_json = ?
                        WHERE task_id = ?
                        """,
                        (
                            TASK_STATUS_FAILED,
                            now_iso,
                            self._dumps(error_payload),
                            row["task_id"],
                        ),
                    )
                    self._close_attempt_tx(
                        conn,
                        row["task_id"],
                        workflow_id_value=row["workflow_id"],
                        status=TASK_STATUS_FAILED,
                        ended_at=now_iso,
                        error_code="lease_expired",
                        error_json=error_payload,
                    )
                    self._append_event_tx(
                        conn,
                        row["workflow_id"],
                        "task.failed",
                        {"task_id": row["task_id"], "node_id": row["node_id"], "error": error_payload},
                        created_at=now_iso,
                    )
                    self._mark_blocked_tasks_tx(conn, row["workflow_id"], now_iso)
                recovered += 1

            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        for wf in touched_workflows:
            self.refresh_workflow_state(wf)
        return recovered

    def refresh_workflow_state(self, workflow_id_value: str) -> str:
        now_iso = iso_utc()
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            wf = conn.execute(
                "SELECT status FROM workflows WHERE workflow_id = ?",
                (workflow_id_value,),
            ).fetchone()
            if not wf:
                conn.rollback()
                return ""
            current = wf["status"]
            counts = conn.execute(
                """
                SELECT status, COUNT(*) AS c
                FROM tasks
                WHERE workflow_id = ?
                GROUP BY status
                """,
                (workflow_id_value,),
            ).fetchall()
            count = {row["status"]: int(row["c"]) for row in counts}
            total = sum(count.values())

            if current == WORKFLOW_STATUS_CANCEL_REQUESTED:
                running = count.get(TASK_STATUS_RUNNING, 0)
                if running > 0:
                    conn.commit()
                    return WORKFLOW_STATUS_CANCEL_REQUESTED
                conn.execute(
                    """
                    UPDATE tasks
                    SET status = ?, finished_at = ?, next_run_at = NULL
                    WHERE workflow_id = ?
                      AND status IN (?, ?)
                    """,
                    (
                        TASK_STATUS_CANCELLED,
                        now_iso,
                        workflow_id_value,
                        TASK_STATUS_QUEUED,
                        TASK_STATUS_RETRY_WAIT,
                    ),
                )
                conn.execute(
                    """
                    UPDATE workflows
                    SET status = ?, completed_at = COALESCE(completed_at, ?)
                    WHERE workflow_id = ?
                    """,
                    (WORKFLOW_STATUS_CANCELLED, now_iso, workflow_id_value),
                )
                self._append_event_tx(
                    conn,
                    workflow_id_value,
                    "workflow.cancelled",
                    {},
                    created_at=now_iso,
                )
                conn.commit()
                return WORKFLOW_STATUS_CANCELLED

            new_status = current
            if total > 0 and count.get(TASK_STATUS_SUCCEEDED, 0) == total:
                new_status = WORKFLOW_STATUS_COMPLETED
            elif count.get(TASK_STATUS_FAILED, 0) > 0:
                new_status = WORKFLOW_STATUS_FAILED
            elif count.get(TASK_STATUS_RUNNING, 0) > 0 or count.get(TASK_STATUS_SUCCEEDED, 0) > 0:
                new_status = WORKFLOW_STATUS_RUNNING
            elif count.get(TASK_STATUS_RETRY_WAIT, 0) > 0:
                new_status = WORKFLOW_STATUS_RUNNING
            else:
                new_status = WORKFLOW_STATUS_QUEUED

            if new_status != current:
                completed_at = now_iso if new_status in (
                    WORKFLOW_STATUS_COMPLETED,
                    WORKFLOW_STATUS_FAILED,
                ) else None
                conn.execute(
                    """
                    UPDATE workflows
                    SET status = ?,
                        completed_at = CASE
                            WHEN ? IS NULL THEN completed_at
                            ELSE COALESCE(completed_at, ?)
                        END
                    WHERE workflow_id = ?
                    """,
                    (new_status, completed_at, completed_at, workflow_id_value),
                )
                self._append_event_tx(
                    conn,
                    workflow_id_value,
                    f"workflow.{new_status}",
                    {},
                    created_at=now_iso,
                )
            conn.commit()
            return new_status
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def cancel_workflow(self, workflow_id_value: str) -> Optional[Dict[str, Any]]:
        now_iso = iso_utc()
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT status FROM workflows WHERE workflow_id = ?",
                (workflow_id_value,),
            ).fetchone()
            if not row:
                conn.rollback()
                return None
            status = row["status"]
            if status in (WORKFLOW_STATUS_COMPLETED, WORKFLOW_STATUS_FAILED, WORKFLOW_STATUS_CANCELLED):
                conn.commit()
                return {"workflow_id": workflow_id_value, "status": status, "changed": False}
            conn.execute(
                """
                UPDATE workflows
                SET status = ?
                WHERE workflow_id = ?
                """,
                (WORKFLOW_STATUS_CANCEL_REQUESTED, workflow_id_value),
            )
            conn.execute(
                """
                UPDATE tasks
                SET status = ?, finished_at = ?, next_run_at = NULL
                WHERE workflow_id = ?
                  AND status IN (?, ?)
                """,
                (
                    TASK_STATUS_CANCELLED,
                    now_iso,
                    workflow_id_value,
                    TASK_STATUS_QUEUED,
                    TASK_STATUS_RETRY_WAIT,
                ),
            )
            self._append_event_tx(
                conn,
                workflow_id_value,
                "workflow.cancel_requested",
                {},
                created_at=now_iso,
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        status = self.refresh_workflow_state(workflow_id_value)
        return {"workflow_id": workflow_id_value, "status": status, "changed": True}

    def resume_workflow(self, workflow_id_value: str) -> Optional[Dict[str, Any]]:
        now_iso = iso_utc()
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT status FROM workflows WHERE workflow_id = ?",
                (workflow_id_value,),
            ).fetchone()
            if not row:
                conn.rollback()
                return None
            status = row["status"]
            if status not in (
                WORKFLOW_STATUS_FAILED,
                WORKFLOW_STATUS_CANCELLED,
                WORKFLOW_STATUS_CANCEL_REQUESTED,
            ):
                conn.commit()
                return {"workflow_id": workflow_id_value, "status": status, "changed": False}

            # Reset resumable tasks; preserve succeeded tasks.
            conn.execute(
                """
                UPDATE tasks
                SET status = ?, next_run_at = ?, finished_at = NULL, lease_owner = NULL, lease_expires_at = NULL
                WHERE workflow_id = ?
                  AND status IN (?, ?, ?)
                  AND attempt_count < max_attempts
                """,
                (
                    TASK_STATUS_RETRY_WAIT,
                    now_iso,
                    workflow_id_value,
                    TASK_STATUS_FAILED,
                    TASK_STATUS_CANCELLED,
                    TASK_STATUS_BLOCKED,
                ),
            )
            conn.execute(
                """
                UPDATE workflows
                SET status = ?, completed_at = NULL, error_json = NULL
                WHERE workflow_id = ?
                """,
                (WORKFLOW_STATUS_RUNNING, workflow_id_value),
            )
            self._append_event_tx(
                conn,
                workflow_id_value,
                "workflow.resumed",
                {},
                created_at=now_iso,
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        status = self.refresh_workflow_state(workflow_id_value)
        return {"workflow_id": workflow_id_value, "status": status, "changed": True}

    def heartbeat_task(
        self,
        task_id_value: str,
        worker_id: str,
        *,
        lease_expires_at: str,
    ) -> bool:
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            updated = conn.execute(
                """
                UPDATE tasks
                SET lease_expires_at = ?
                WHERE task_id = ? AND status = ? AND lease_owner = ?
                """,
                (lease_expires_at, task_id_value, TASK_STATUS_RUNNING, worker_id),
            )
            conn.commit()
            return updated.rowcount == 1
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def list_registry_models(
        self,
        *,
        modality: Optional[str] = None,
        provider: Optional[str] = None,
        limit: int = 500,
    ) -> List[Dict[str, Any]]:
        limit = max(1, min(2000, int(limit)))
        conn = self._connect()
        try:
            clauses = []
            args: List[Any] = []
            if modality:
                clauses.append("modality = ?")
                args.append(modality.strip().lower())
            if provider:
                clauses.append("provider = ?")
                args.append(provider.strip().lower())
            query = """
                SELECT registry_key, modality, provider, model_id, display_name,
                       price_input_per_m, price_output_per_m, price_unit_json,
                       median_latency_ms, throughput_tps, context_window,
                       benchmark_json, capabilities_json, policy_tags_json,
                       source_name, source_ref, synced_at
                FROM model_registry
            """
            if clauses:
                query += " WHERE " + " AND ".join(clauses)
            query += " ORDER BY modality ASC, provider ASC, model_id ASC LIMIT ?"
            args.append(limit)
            rows = conn.execute(query, tuple(args)).fetchall()
            out = []
            for row in rows:
                out.append(
                    {
                        "registry_key": row["registry_key"],
                        "modality": row["modality"],
                        "provider": row["provider"],
                        "model_id": row["model_id"],
                        "display_name": row["display_name"] or row["model_id"],
                        "price_input_per_m": row["price_input_per_m"],
                        "price_output_per_m": row["price_output_per_m"],
                        "price_unit": self._loads(row["price_unit_json"], {}) or {},
                        "median_latency_ms": row["median_latency_ms"],
                        "throughput_tps": row["throughput_tps"],
                        "context_window": row["context_window"],
                        "benchmark": self._loads(row["benchmark_json"], {}) or {},
                        "capabilities": self._loads(row["capabilities_json"], {}) or {},
                        "policy_tags": self._loads(row["policy_tags_json"], []) or [],
                        "source_name": row["source_name"] or "",
                        "source_ref": row["source_ref"] or "",
                        "synced_at": row["synced_at"],
                    }
                )
            return out
        finally:
            conn.close()

    def seed_registry_if_empty(
        self,
        rows: List[Dict[str, Any]],
        *,
        source_name: str = "seed",
        source_ref: str = "",
        checksum: str = "",
    ) -> bool:
        result = self.replace_registry_snapshot(
            rows, source_name=source_name, source_ref=source_ref,
            checksum=checksum, only_if_empty=True,
        )
        return result["status"] == "succeeded"

    def replace_registry_snapshot(
        self,
        rows: List[Dict[str, Any]],
        *,
        source_name: str = "manual",
        source_ref: str = "",
        checksum: str = "",
        only_if_empty: bool = False,
        previous_seeds: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        if not isinstance(rows, list):
            raise WorkflowValidationError("Registry rows must be an array.")
        now_iso = iso_utc()
        job = f"sync_{uuid.uuid4().hex[:12]}"
        normalized_rows: List[Dict[str, Any]] = []
        for raw in rows:
            if not isinstance(raw, dict):
                continue
            modality = (raw.get("modality") or "").strip().lower()
            provider = (raw.get("provider") or "").strip().lower()
            model_id = (raw.get("model_id") or raw.get("model") or "").strip()
            if modality not in ("llm", "image", "video", "parse"):
                continue
            if not provider or not model_id:
                continue
            registry_key = (raw.get("registry_key") or f"{modality}:{provider}:{model_id}").strip()
            try:
                context_window = int(raw.get("context_window")) if raw.get("context_window") not in (None, "") else None
            except Exception:
                context_window = None
            normalized_rows.append(
                {
                    "registry_key": registry_key,
                    "modality": modality,
                    "provider": provider,
                    "model_id": model_id,
                    "display_name": (raw.get("display_name") or "").strip(),
                    "price_input_per_m": float(raw.get("price_input_per_m")) if raw.get("price_input_per_m") not in (None, "") else None,
                    "price_output_per_m": float(raw.get("price_output_per_m")) if raw.get("price_output_per_m") not in (None, "") else None,
                    "price_unit_json": self._dumps(raw.get("price_unit") or raw.get("price_unit_json") or {}),
                    "median_latency_ms": float(raw.get("median_latency_ms")) if raw.get("median_latency_ms") not in (None, "") else None,
                    "throughput_tps": float(raw.get("throughput_tps")) if raw.get("throughput_tps") not in (None, "") else None,
                    "context_window": context_window,
                    "benchmark_json": self._dumps(raw.get("benchmark") or raw.get("benchmark_json") or {}),
                    "capabilities_json": self._dumps(raw.get("capabilities") or raw.get("capabilities_json") or {}),
                    "policy_tags_json": self._dumps(raw.get("policy_tags") or raw.get("policy_tags_json") or []),
                }
            )

        if not normalized_rows:
            raise WorkflowValidationError("Registry snapshot has no valid rows.")

        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            previous_rows = []
            if only_if_empty or previous_seeds is not None:
                previous_rows = [dict(row) for row in conn.execute("SELECT * FROM model_registry").fetchall()]
                if only_if_empty and previous_rows:
                    conn.rollback()
                    return {"status": "unchanged"}
            if previous_seeds is not None:
                # Refresh only a recognized built-in snapshot. Manual syncs have
                # their own checksum/provenance and must never be overwritten.
                latest = conn.execute(
                    "SELECT source_name, checksum, details_json FROM sync_jobs "
                    "WHERE status = 'succeeded' ORDER BY rowid DESC LIMIT 1"
                ).fetchone()
                active_rows = [
                    row for row in previous_rows
                    if "seed_pin_only" not in (self._loads(row["policy_tags_json"], []) or [])
                ]
                details = self._loads(latest["details_json"], {}) if latest else {}
                matched = any(
                    isinstance(seed, dict)
                    and bool(seed.get("checksum"))
                    and latest is not None
                    and seed.get("checksum") == latest["checksum"]
                    and seed.get("source_name") == latest["source_name"]
                    and seed.get("source_ref") == details.get("source_ref")
                    and len(active_rows) == details.get("row_count")
                    and all(
                        row["source_name"] == seed.get("source_name")
                        and row["source_ref"] == seed.get("source_ref")
                        for row in active_rows
                    )
                    for seed in previous_seeds
                )
                if not active_rows or not matched:
                    conn.rollback()
                    return {"status": "unchanged"}
            sync_details = {"row_count": len(normalized_rows), "source_ref": source_ref}
            if previous_seeds is not None:
                # Preserve original metrics and provenance without assigning
                # historical measurements to a different model ID.
                sync_details["previous_snapshot"] = previous_rows
            conn.execute(
                """
                INSERT INTO sync_jobs (job_id, source_name, started_at, status, checksum, details_json)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    job,
                    source_name,
                    now_iso,
                    "running",
                    checksum or None,
                    self._dumps(sync_details),
                ),
            )
            conn.execute("DELETE FROM model_registry")
            snapshot_rows = [
                {**row, "source_name": source_name, "source_ref": source_ref or None, "synced_at": now_iso}
                for row in normalized_rows
            ]
            if previous_seeds is not None:
                current_keys = {row["registry_key"] for row in normalized_rows}
                for previous in previous_rows:
                    if previous["registry_key"] in current_keys:
                        continue
                    tags = self._loads(previous["policy_tags_json"], []) or []
                    if "seed_pin_only" not in tags:
                        tags.append("seed_pin_only")
                    previous["policy_tags_json"] = self._dumps(tags)
                    snapshot_rows.append(previous)
            for row in snapshot_rows:
                conn.execute(
                    """
                    INSERT INTO model_registry (
                        registry_key, modality, provider, model_id, display_name,
                        price_input_per_m, price_output_per_m, price_unit_json,
                        median_latency_ms, throughput_tps, context_window,
                        benchmark_json, capabilities_json, policy_tags_json,
                        source_name, source_ref, synced_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        row["registry_key"],
                        row["modality"],
                        row["provider"],
                        row["model_id"],
                        row["display_name"] or None,
                        row["price_input_per_m"],
                        row["price_output_per_m"],
                        row["price_unit_json"],
                        row["median_latency_ms"],
                        row["throughput_tps"],
                        row["context_window"],
                        row["benchmark_json"],
                        row["capabilities_json"],
                        row["policy_tags_json"],
                        row["source_name"],
                        row["source_ref"],
                        row["synced_at"],
                    ),
                )
            conn.execute(
                """
                UPDATE sync_jobs
                SET status = ?, completed_at = ?
                WHERE job_id = ?
                """,
                ("succeeded", now_iso, job),
            )
            conn.commit()
        except Exception as exc:
            conn.rollback()
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    """
                    INSERT OR REPLACE INTO sync_jobs (job_id, source_name, started_at, completed_at, status, checksum, details_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        job,
                        source_name,
                        now_iso,
                        iso_utc(),
                        "failed",
                        checksum or None,
                        self._dumps({"error": str(exc), "source_ref": source_ref}),
                    ),
                )
                conn.commit()
            except Exception:
                conn.rollback()
            raise
        finally:
            conn.close()
        return {"job_id": job, "status": "succeeded", "row_count": len(normalized_rows), "completed_at": now_iso}

    def list_sync_jobs(self, *, limit: int = 20) -> List[Dict[str, Any]]:
        limit = max(1, min(200, int(limit)))
        conn = self._connect()
        try:
            rows = conn.execute(
                """
                SELECT job_id, source_name, started_at, completed_at, status, checksum, details_json
                FROM sync_jobs
                ORDER BY started_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [
                {
                    "job_id": row["job_id"],
                    "source_name": row["source_name"],
                    "started_at": row["started_at"],
                    "completed_at": row["completed_at"],
                    "status": row["status"],
                    "checksum": row["checksum"] or "",
                    "details": self._loads(row["details_json"], {}) or {},
                }
                for row in rows
            ]
        finally:
            conn.close()

    def list_open_provider_circuits(
        self,
        *,
        modality: Optional[str] = None,
        now: Optional[str] = None,
    ) -> Dict[str, str]:
        now_iso = now or iso_utc()
        conn = self._connect()
        try:
            args: List[Any] = [now_iso]
            clause = ""
            if modality:
                clause = " AND modality = ?"
                args.append(modality.strip().lower())
            rows = conn.execute(
                f"""
                SELECT provider_key, modality
                FROM provider_health
                WHERE circuit_state = 'open'
                  AND reopen_at IS NOT NULL
                  AND reopen_at > ?{clause}
                """,
                tuple(args),
            ).fetchall()
            return {str(row["provider_key"]): str(row["modality"]) for row in rows}
        finally:
            conn.close()

    def record_provider_success(self, *, provider: str, modality: str) -> None:
        provider_key = (provider or "").strip().lower()
        modality_key = (modality or "").strip().lower()
        if not provider_key or not modality_key:
            return
        now_iso = iso_utc()
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                INSERT INTO provider_health (provider_key, modality, consecutive_failures, circuit_state, reopen_at, updated_at)
                VALUES (?, ?, 0, 'closed', NULL, ?)
                ON CONFLICT(provider_key, modality) DO UPDATE SET
                    consecutive_failures = 0,
                    circuit_state = 'closed',
                    reopen_at = NULL,
                    updated_at = excluded.updated_at
                """,
                (provider_key, modality_key, now_iso),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def record_provider_failure(
        self,
        *,
        provider: str,
        modality: str,
        transient: bool,
        threshold: int = 3,
        cooldown_seconds: int = 60,
    ) -> None:
        provider_key = (provider or "").strip().lower()
        modality_key = (modality or "").strip().lower()
        if not provider_key or not modality_key:
            return
        now_dt = now_utc()
        now_iso = iso_utc(now_dt)
        reopen_at = iso_utc(now_dt + timedelta(seconds=max(5, int(cooldown_seconds))))
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """
                SELECT consecutive_failures, circuit_state, reopen_at
                FROM provider_health
                WHERE provider_key = ? AND modality = ?
                """,
                (provider_key, modality_key),
            ).fetchone()
            failures = int(row["consecutive_failures"] or 0) if row else 0
            failures = failures + 1 if transient else failures
            state = "open" if transient and failures >= max(1, int(threshold)) else "closed"
            target_reopen = reopen_at if state == "open" else None
            conn.execute(
                """
                INSERT INTO provider_health (provider_key, modality, consecutive_failures, circuit_state, reopen_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(provider_key, modality) DO UPDATE SET
                    consecutive_failures = excluded.consecutive_failures,
                    circuit_state = excluded.circuit_state,
                    reopen_at = excluded.reopen_at,
                    updated_at = excluded.updated_at
                """,
                (
                    provider_key,
                    modality_key,
                    failures,
                    state,
                    target_reopen,
                    now_iso,
                ),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
