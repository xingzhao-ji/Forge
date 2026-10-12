"""HTTP route adapters for Forge APIs."""

from __future__ import annotations

import json
import re
import time
from typing import Optional

from forge.runtime import WorkflowConflictError, WorkflowValidationError


WORKFLOW_ID_RE = re.compile(r"^[A-Za-z0-9_.:-]{6,128}$")


def _runtime_or_503(handler, runtime):
    if runtime is None:
        handler.send_json({"ok": False, "error": "Forge runtime unavailable."}, status=503)
        return None
    return runtime


def _read_json(handler):
    length = int(handler.headers.get("Content-Length", 0) or 0)
    if length <= 0:
        return None, "Missing payload."
    try:
        body = handler.rfile.read(length)
        payload = json.loads(body)
    except json.JSONDecodeError:
        return None, "Invalid JSON."
    if not isinstance(payload, dict):
        return None, "Payload must be an object."
    return payload, None


def _workflow_id_from_path(path: str, suffix: str = "") -> Optional[str]:
    if suffix:
        if not path.endswith(suffix):
            return None
        base = path[: -len(suffix)]
    else:
        base = path
    prefix = "/api/workflows/"
    if not base.startswith(prefix):
        return None
    value = base[len(prefix):].strip().strip("/")
    if not value or not WORKFLOW_ID_RE.match(value):
        return None
    return value


def _sse_write_json(handler, *, event: str, data: dict, event_id: Optional[int] = None) -> None:
    lines = []
    if event_id is not None:
        lines.append(f"id: {int(event_id)}")
    if event:
        lines.append(f"event: {event}")
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    for line in payload.splitlines() or ["{}"]:
        lines.append(f"data: {line}")
    raw = ("\n".join(lines) + "\n\n").encode("utf-8")
    handler.wfile.write(raw)
    handler.wfile.flush()


def handle_templates(handler, runtime, query=None):
    rt = _runtime_or_503(handler, runtime)
    if not rt:
        return
    query = query or {}
    template_id = (query.get("template_id") or [""])[0].strip() or None
    latest_raw = (query.get("latest_only") or ["1"])[0].strip().lower()
    latest_only = latest_raw not in ("0", "false", "no")
    version_raw = (query.get("version") or [""])[0].strip()
    version = None
    if version_raw:
        try:
            version = int(version_raw)
        except Exception:
            return handler.send_json({"ok": False, "error": "version must be an integer."}, status=400)
    templates = rt.list_templates(template_id=template_id, version=version, latest_only=latest_only)
    handler.send_json(
        {
            "ok": True,
            "schema_version": rt.templates_schema_version(),
            "templates": templates,
        }
    )


def handle_workflows_list(handler, runtime, query):
    rt = _runtime_or_503(handler, runtime)
    if not rt:
        return
    status = (query.get("status") or [""])[0].strip() or None
    task_type = (query.get("task_type") or [""])[0].strip() or None
    limit_raw = (query.get("limit") or ["50"])[0]
    try:
        limit = int(limit_raw)
    except Exception:
        limit = 50
    workflows = rt.list_workflows(status=status, task_type=task_type, limit=limit)
    handler.send_json({"ok": True, "workflows": workflows})


def handle_workflows_create(handler, runtime):
    rt = _runtime_or_503(handler, runtime)
    if not rt:
        return
    payload, err = _read_json(handler)
    if err:
        handler.send_json({"ok": False, "error": err}, status=400)
        return
    try:
        result = rt.create_workflow(payload, submitted_by="web")
    except WorkflowValidationError as exc:
        handler.send_json({"ok": False, "error": str(exc)}, status=400)
        return
    except WorkflowConflictError as exc:
        handler.send_json({"ok": False, "error": str(exc)}, status=409)
        return
    status_code = 201 if result.get("created") else 200
    handler.send_json(
        {
            "ok": True,
            "workflow_id": result["workflow_id"],
            "status": result["status"],
            "task_count": result["task_count"],
            "node_count": result["task_count"],
            "objective": result.get("objective") or "balanced",
            "accepted_at": result["accepted_at"],
            "budget_usd": result.get("budget_usd"),
            "spent_usd": result.get("spent_usd", 0.0),
            "created": bool(result.get("created")),
        },
        status=status_code,
    )


def handle_workflow_detail(handler, runtime, path):
    rt = _runtime_or_503(handler, runtime)
    if not rt:
        return
    wf_id = _workflow_id_from_path(path)
    if not wf_id:
        handler.send_json({"ok": False, "error": "Invalid workflow id."}, status=400)
        return
    detail = rt.get_workflow_detail(wf_id)
    if not detail:
        handler.send_json({"ok": False, "error": "Workflow not found."}, status=404)
        return
    handler.send_json({"ok": True, **detail})


def handle_workflow_events(handler, runtime, path, query):
    rt = _runtime_or_503(handler, runtime)
    if not rt:
        return
    wf_id = _workflow_id_from_path(path, suffix="/events")
    if not wf_id:
        handler.send_json({"ok": False, "error": "Invalid workflow id."}, status=400)
        return
    try:
        after = int((query.get("after") or ["0"])[0])
    except Exception:
        after = 0
    try:
        limit = int((query.get("limit") or ["200"])[0])
    except Exception:
        limit = 200
    events = rt.get_workflow_events(wf_id, after_event_id=after, limit=limit)
    handler.send_json({"ok": True, "workflow_id": wf_id, "events": events})


def handle_workflow_events_stream(handler, runtime, path, query):
    rt = _runtime_or_503(handler, runtime)
    if not rt:
        return
    wf_id = _workflow_id_from_path(path, suffix="/events/stream")
    if not wf_id:
        handler.send_json({"ok": False, "error": "Invalid workflow id."}, status=400)
        return

    # Optional existence check up front so clients receive 404 immediately.
    if not rt.get_workflow_detail(wf_id):
        handler.send_json({"ok": False, "error": "Workflow not found."}, status=404)
        return

    try:
        after = int((query.get("after") or ["0"])[0])
    except Exception:
        after = 0
    try:
        after = max(after, int((handler.headers.get("Last-Event-ID") or "0").strip() or "0"))
    except Exception:
        pass

    try:
        timeout_seconds = int((query.get("timeout") or ["25"])[0])
    except Exception:
        timeout_seconds = 25
    timeout_seconds = max(5, min(60, timeout_seconds))

    try:
        poll_ms = int((query.get("poll_ms") or ["900"])[0])
    except Exception:
        poll_ms = 900
    poll_ms = max(250, min(5000, poll_ms))
    poll_seconds = poll_ms / 1000.0

    handler.send_response(200)
    handler.send_header("Content-Type", "text/event-stream; charset=utf-8")
    handler.send_header("Cache-Control", "no-cache, no-transform")
    handler.send_header("Connection", "keep-alive")
    handler.send_header("X-Accel-Buffering", "no")
    handler.end_headers()

    cursor = after
    deadline = time.time() + timeout_seconds
    last_keepalive = 0.0
    try:
        handler.wfile.write(b"retry: 1500\n\n")
        handler.wfile.flush()
        while time.time() < deadline:
            events = rt.get_workflow_events(wf_id, after_event_id=cursor, limit=200)
            if events:
                for event in events:
                    event_id = int(event.get("event_id") or 0)
                    cursor = max(cursor, event_id)
                    _sse_write_json(
                        handler,
                        event="workflow_event",
                        data=event,
                        event_id=event_id,
                    )
                detail = rt.get_workflow_detail(wf_id) or {}
                status = ((detail.get("workflow") or {}).get("status") or "").strip().lower()
                if status in ("completed", "failed", "cancelled"):
                    _sse_write_json(
                        handler,
                        event="workflow_terminal",
                        data={"workflow_id": wf_id, "status": status},
                        event_id=cursor or None,
                    )
                    break
                continue

            if (time.time() - last_keepalive) >= 10.0:
                handler.wfile.write(b": keep-alive\n\n")
                handler.wfile.flush()
                last_keepalive = time.time()
            time.sleep(poll_seconds)
        _sse_write_json(handler, event="stream_end", data={"workflow_id": wf_id, "after": cursor})
    except (BrokenPipeError, ConnectionResetError, OSError):
        return


def handle_workflow_cancel(handler, runtime, path):
    rt = _runtime_or_503(handler, runtime)
    if not rt:
        return
    wf_id = _workflow_id_from_path(path, suffix="/cancel")
    if not wf_id:
        handler.send_json({"ok": False, "error": "Invalid workflow id."}, status=400)
        return
    result = rt.cancel_workflow(wf_id)
    if not result:
        handler.send_json({"ok": False, "error": "Workflow not found."}, status=404)
        return
    handler.send_json({"ok": True, **result})


def handle_workflow_resume(handler, runtime, path):
    rt = _runtime_or_503(handler, runtime)
    if not rt:
        return
    wf_id = _workflow_id_from_path(path, suffix="/resume")
    if not wf_id:
        handler.send_json({"ok": False, "error": "Invalid workflow id."}, status=400)
        return
    result = rt.resume_workflow(wf_id)
    if not result:
        handler.send_json({"ok": False, "error": "Workflow not found."}, status=404)
        return
    handler.send_json({"ok": True, **result})


def handle_registry_list(handler, runtime, query):
    rt = _runtime_or_503(handler, runtime)
    if not rt:
        return
    modality = (query.get("modality") or [""])[0].strip().lower() or None
    provider = (query.get("provider") or [""])[0].strip().lower() or None
    limit_raw = (query.get("limit") or ["200"])[0]
    try:
        limit = int(limit_raw)
    except Exception:
        limit = 200
    models = rt.list_registry_models(modality=modality, provider=provider, limit=limit)
    jobs = rt.list_registry_sync_jobs(limit=10)
    handler.send_json({"ok": True, "models": models, "sync_jobs": jobs})


def handle_registry_sync(handler, runtime):
    rt = _runtime_or_503(handler, runtime)
    if not rt:
        return
    payload, err = _read_json(handler)
    if err:
        payload = {}
    try:
        result = rt.sync_registry(payload if isinstance(payload, dict) else {})
    except WorkflowValidationError as exc:
        handler.send_json({"ok": False, "error": str(exc)}, status=400)
        return
    except Exception as exc:
        handler.send_json({"ok": False, "error": str(exc)}, status=500)
        return
    handler.send_json({"ok": True, **result})


def handle_routing_preview(handler, runtime):
    rt = _runtime_or_503(handler, runtime)
    if not rt:
        return
    payload, err = _read_json(handler)
    if err:
        handler.send_json({"ok": False, "error": err}, status=400)
        return
    task_type = (payload.get("task_type") or "").strip()
    if not task_type:
        handler.send_json({"ok": False, "error": "task_type is required."}, status=400)
        return
    task_params = payload.get("params") if isinstance(payload.get("params"), dict) else {}
    routing = payload.get("routing") if isinstance(payload.get("routing"), dict) else {}
    routing_override = payload.get("routing_override") if isinstance(payload.get("routing_override"), dict) else {}
    plan = rt.preview_routing(
        task_type=task_type,
        task_params=task_params,
        routing=routing,
        routing_override=routing_override,
    )
    handler.send_json({"ok": True, "plan": plan})
