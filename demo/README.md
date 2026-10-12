# Restricted live web demonstration

This wrapper runs **the unchanged Forge Python/SQLite runtime**, including its
scheduler and four-worker dispatcher. It is not a browser port or a simulation.
The only extra executor is `demo.retry_once`: a labeled synthetic failure fixture.
It raises a transient error on attempt one so Forge's original retry machinery
can be observed. The other tasks are the repository's bundled text executors.

## Run locally

From the repository root, with Gunicorn installed:

```sh
python -m pip install gunicorn==23.0.0
gunicorn --bind 127.0.0.1:8080 --workers 1 --threads 8 demo.server:app
```

Open http://127.0.0.1:8080. Or build the included Dockerfile and publish port 8080.
The container runs as a non-root user and requires no credentials or database.
It accepts Cloud Run's `PORT` environment variable.

## Hosting requirements

This needs a real Python process. GitHub Pages can link to it, but cannot execute
its backend. Current Pyodide cannot start the Python scheduler thread or worker
threads, so presenting a synchronous browser replacement as this runtime would
misrepresent its behavior. See the [official Pyodide limitations](https://pyodide.org/en/stable/usage/wasm-constraints.html).

Keep exactly **one Gunicorn worker and one serving instance** because demo run
lookup is process-local. Use a managed HTTPS reverse proxy with request limits.
For Cloud Run, configure a single instance, concurrency 8, and bounded memory and
CPU. Min instances can be zero; cold starts and server recycling discard demo
runs. The scheduler requires CPU between HTTP requests: use instance-based CPU
allocation (with separately approved costs). Request-based CPU can stall
background progress, even with polling, and is not a suitable runtime guarantee.
Do not claim persistence across restarts or multi-instance support. Deploying can
incur hosting/build costs and requires the account owner's authorization.

## Public surface and bounds

- POST `/api/runs` accepts only `{"scenario":"pipeline"}` or `{"scenario":"retry"}`.
- GET `/api/runs/{random-token}` returns that synthetic run's actual database state.
- POST `/api/runs/{random-token}/replay` repeats its exact original submission and
  reports the actual workflow IDs, exercising Forge's persisted idempotency check.
- No arbitrary Python, task graph, filesystem path, user text, provider, upload,
  external URL, authentication secret, or third-party API call is accepted.
- Four active sessions maximum; 12 starts per rolling minute across all users.
- Completed sessions can be evicted for capacity; sessions expire after 10 minutes.
  Expiration cleanup occurs on requests. All state is temporary, bounded per process.
- Session IDs are random bearer tokens. There is no listing endpoint or CORS grant.
- This is a restricted synthetic demo, not a general-purpose public workflow API.
  It does not expose the repository's unrestricted HTTP adapters.

## Verification

`python -m unittest discover -s tests -v` runs the original 28 tests plus four
wrapper tests against real SQLite files, real scheduler/worker threads, retries,
idempotency, rejection of arbitrary input, response headers, and rate limits.
The container image and remote cloud deployment must be verified separately.
