"""Run a real, synthetic two-task workflow without external services."""
import argparse
import json
from pathlib import Path
import tempfile
import time

from forge import ForgeRuntime


def run_demo(root: Path, timeout: float) -> int:
    with ForgeRuntime(root) as runtime:
        created = runtime.create_workflow({
            "name": "Synthetic text pipeline",
            "nodes": [
                {"id": "parse", "type": "parse.text.extract",
                 "params": {"text": "Forge runs dependent tasks locally."}},
                {"id": "count", "type": "text.stats", "depends_on": ["parse"]},
            ],
        })
        workflow_id = created["workflow_id"]
        deadline = time.monotonic() + timeout
        cursor = 0
        while time.monotonic() < deadline:
            for event in runtime.get_workflow_events(workflow_id, after_event_id=cursor):
                cursor = event["event_id"]
                print(json.dumps({"event_id": cursor, "event": event["event_type"]}), flush=True)
            detail = runtime.get_workflow_detail(workflow_id)
            status = detail["workflow"]["status"]
            if status in {"completed", "failed", "cancelled"}:
                print(json.dumps({"workflow_id": workflow_id, "status": status,
                                  "tasks": [{"node": t["node_id"], "status": t["status"],
                                             "outputs": t.get("outputs")} for t in detail["tasks"]]}, indent=2))
                return 0 if status == "completed" else 1
            time.sleep(0.05)
        runtime.cancel_workflow(workflow_id)
        print(json.dumps({"workflow_id": workflow_id, "status": "timeout"}))
        return 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, help="Optional directory to retain this demo's SQLite database")
    parser.add_argument("--timeout", type=float, default=10.0)
    args = parser.parse_args()
    if not 0 < args.timeout <= 120:
        parser.error("--timeout must be between 0 and 120 seconds")
    if args.data_dir:
        return run_demo(args.data_dir, args.timeout)
    with tempfile.TemporaryDirectory(prefix="forge-demo-") as directory:
        return run_demo(Path(directory), args.timeout)


if __name__ == "__main__":
    raise SystemExit(main())
