"""Allowlisted, bounded WSGI demo. No arbitrary DAGs, code, paths, or provider calls."""
from __future__ import annotations
import json
import secrets
import tempfile
import threading
import time
from collections import deque
from pathlib import Path
from forge import ForgeRuntime
from forge.executors import BaseExecutor, ForgeTaskError
from forge.models import ExecutionResult


class RetryFixture(BaseExecutor):
    """Demo-only executor: deliberately fail its first persisted attempt."""
    task_type = "demo.retry_once"

    def execute(self, ctx, task):
        if task["attempt_count"] == 1:
            raise ForgeTaskError("Intentional demo failure; retry is handled by Forge.",
                                 code="demo_transient", transient=True)
        return ExecutionResult(outputs={"text": "Forge runs dependent tasks locally."},
                               artifacts=[], logs=["Second attempt succeeded."], provider_used="local")


class DemoApp:
    def __init__(self, *, max_sessions=4, ttl=600):
        self.lock = threading.RLock()
        self.sessions = {}
        self.starts = deque()
        self.max_sessions = max_sessions
        self.ttl = ttl

    def close(self):
        with self.lock:
            for session in self.sessions.values():
                session["runtime"].stop()
                session["directory"].cleanup()
            self.sessions.clear()

    def _expire(self):
        now = time.monotonic()
        for token, session in list(self.sessions.items()):
            if now - session["created"] > self.ttl:
                session["runtime"].stop()
                session["directory"].cleanup()
                del self.sessions[token]
        while self.starts and now - self.starts[0] > 60:
            self.starts.popleft()

    def _create(self, scenario):
        self._expire()
        if len(self.starts) >= 12:
            return 429, {"error": "Demo rate limit reached. Try again in one minute."}
        if len(self.sessions) >= self.max_sessions:
            # Only evict completed runs, never interrupt a live workflow.
            for token, old in list(self.sessions.items()):
                state = old["runtime"].get_workflow_detail(old["workflow_id"])["workflow"]["status"]
                if state in {"completed", "failed", "cancelled"}:
                    old["runtime"].stop()
                    old["directory"].cleanup()
                    del self.sessions[token]
                    break
        if len(self.sessions) >= self.max_sessions:
            return 429, {"error": "All demo slots are busy. Try again shortly."}
        directory = tempfile.TemporaryDirectory(prefix="forge-public-demo-")
        runtime = ForgeRuntime(directory.name)
        runtime.registry.register(RetryFixture())
        token = secrets.token_urlsafe(32)
        first = ({"id": "retry", "type": "demo.retry_once"} if scenario == "retry" else
                 {"id": "parse", "type": "parse.text.extract", "params": {"text": "Forge runs dependent tasks locally."}})
        payload = {"name": "Public synthetic " + scenario, "idempotency_key": token,
                   "nodes": [first, {"id": "count", "type": "text.stats", "depends_on": [first["id"]]}]}
        try:
            created = runtime.create_workflow(payload)
            runtime.start()
        except Exception:
            runtime.stop()
            directory.cleanup()
            raise
        self.sessions[token] = {"runtime": runtime, "directory": directory, "payload": payload,
                                "workflow_id": created["workflow_id"], "created": time.monotonic()}
        self.starts.append(time.monotonic())
        return 201, {"token": token, "workflow_id": created["workflow_id"], "scenario": scenario}

    def route(self, environ):
        method, path = environ.get("REQUEST_METHOD"), environ.get("PATH_INFO", "/")
        if method == "GET" and path == "/":
            return 200, Path(__file__).with_name("index.html").read_bytes(), "text/html; charset=utf-8"
        if method == "GET" and path == "/healthz":
            return 200, {"status": "ok", "engine": "original Forge Python/SQLite runtime"}, None
        with self.lock:
            self._expire()
            if method == "POST" and path == "/api/runs":
                if environ.get("CONTENT_TYPE", "").split(";")[0] != "application/json":
                    return 415, {"error": "Expected application/json"}, None
                try:
                    length = int(environ.get("CONTENT_LENGTH") or 0)
                    if not 0 < length <= 128:
                        return 413, {"error": "Request must be 1–128 bytes"}, None
                    payload = json.loads(environ["wsgi.input"].read(length))
                except (ValueError, UnicodeError):
                    return 400, {"error": "Invalid JSON"}, None
                if not isinstance(payload, dict) or set(payload) != {"scenario"} or payload["scenario"] not in ("pipeline", "retry"):
                    return 400, {"error": "Choose pipeline or retry; no other inputs are accepted"}, None
                code, body = self._create(payload["scenario"])
                return code, body, None
            parts = path.strip("/").split("/")
            if len(parts) in (3, 4) and parts[:2] == ["api", "runs"]:
                session = self.sessions.get(parts[2])
                if not session:
                    return 404, {"error": "Run not found or expired. Start a new run."}, None
                runtime, wid = session["runtime"], session["workflow_id"]
                if method == "GET" and len(parts) == 3:
                    return 200, {"detail": runtime.get_workflow_detail(wid),
                                 "events": runtime.get_workflow_events(wid), "workers": 4}, None
                if method == "POST" and len(parts) == 4 and parts[3] == "replay":
                    repeated = runtime.create_workflow(session["payload"])
                    return 200, {"original_workflow_id": wid, "returned_workflow_id": repeated["workflow_id"],
                                 "same_workflow": repeated["workflow_id"] == wid}, None
        return 404, {"error": "Not found"}, None

    def __call__(self, environ, start_response):
        try:
            code, body, content_type = self.route(environ)
        except Exception:
            code, body, content_type = 500, {"error": "Demo runtime error"}, None
        data = body if isinstance(body, bytes) else json.dumps(body).encode()
        labels = {200: "OK", 201: "Created", 400: "Bad Request", 404: "Not Found", 413: "Payload Too Large", 415: "Unsupported Media Type", 429: "Too Many Requests", 500: "Internal Server Error"}
        start_response(f"{code} {labels[code]}", [("Content-Type", content_type or "application/json"),
            ("Content-Length", str(len(data))), ("Cache-Control", "no-store"),
            ("X-Content-Type-Options", "nosniff"), ("Referrer-Policy", "no-referrer"),
            ("Content-Security-Policy", "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; frame-ancestors 'none'; base-uri 'none'"),
            ("Permissions-Policy", "camera=(), microphone=(), geolocation=()")])
        return [data]


app = DemoApp()
