"""Exercise the restricted wrapper against real scheduler threads and SQLite."""
import io
import json
import time
import unittest
from demo.server import DemoApp


class PublicDemoTests(unittest.TestCase):
    def setUp(self):
        self.app = DemoApp(max_sessions=2)

    def tearDown(self):
        self.app.close()

    def request(self, path, payload=None, method=None, content_type="application/json"):
        data = json.dumps(payload).encode() if payload is not None else b""
        response = []
        body = b"".join(self.app({"PATH_INFO": path, "REQUEST_METHOD": method or ("POST" if payload is not None else "GET"),
            "CONTENT_TYPE": content_type, "CONTENT_LENGTH": str(len(data)), "wsgi.input": io.BytesIO(data)},
            lambda status, headers: response.append((status, dict(headers)))))
        return int(response[0][0].split()[0]), json.loads(body), response[0][1]

    def wait(self, token):
        deadline = time.monotonic() + 12
        while time.monotonic() < deadline:
            code, data, _ = self.request('/api/runs/' + token)
            self.assertEqual(code, 200)
            if data['detail']['workflow']['status'] in ('completed', 'failed', 'cancelled'):
                return data
            time.sleep(.05)
        self.fail('Actual runtime did not finish')

    def test_pipeline_idempotency_and_real_sqlite(self):
        code, made, _ = self.request('/api/runs', {'scenario': 'pipeline'})
        self.assertEqual(code, 201)
        data = self.wait(made['token'])
        self.assertEqual(data['detail']['workflow']['status'], 'completed')
        count = next(t for t in data['detail']['tasks'] if t['node_id'] == 'count')
        self.assertEqual(count['outputs']['words'], 5)
        self.assertTrue(data['events'])
        session = self.app.sessions[made['token']]
        self.assertEqual(session['runtime'].dispatcher.pool._max_workers, 4)
        with open(session['runtime'].db_path, 'rb') as db:
            self.assertEqual(db.read(16), b'SQLite format 3\x00')
        code, repeated, _ = self.request('/api/runs/' + made['token'] + '/replay', method='POST')
        self.assertEqual(code, 200)
        self.assertTrue(repeated['same_workflow'])
        self.assertEqual(len(session['runtime'].list_workflows()), 1)

    def test_actual_transient_retry_attempts(self):
        _, made, _ = self.request('/api/runs', {'scenario': 'retry'})
        data = self.wait(made['token'])
        self.assertEqual(data['detail']['workflow']['status'], 'completed')
        retry = next(t for t in data['detail']['tasks'] if t['node_id'] == 'retry')
        self.assertEqual(retry['attempt_count'], 2)
        self.assertEqual(len(retry['attempts']), 2)
        self.assertIn('task.retry_scheduled', [e['event_type'] for e in data['events']])

    def test_rejects_arbitrary_payload_and_routes(self):
        for payload in ({'scenario': 'exec'}, {'scenario': 'pipeline', 'nodes': []}, [], {'scenario': {'bad': True}}):
            self.assertEqual(self.request('/api/runs', payload)[0], 400)
        self.assertEqual(self.request('/api/runs', {'scenario': 'pipeline'}, content_type='text/plain')[0], 415)
        self.assertEqual(self.request('/api/runs', {'scenario': 'x' * 300})[0], 413)
        self.assertEqual(self.request('/api/workflows')[0], 404)
        self.assertEqual(self.request('/api/runs/not-a-token')[0], 404)

    def test_headers_and_rate_limit(self):
        _, _, headers = self.request('/healthz')
        self.assertEqual(headers['Cache-Control'], 'no-store')
        self.assertNotIn('Access-Control-Allow-Origin', headers)
        self.app.starts.extend([time.monotonic()] * 12)
        self.assertEqual(self.request('/api/runs', {'scenario': 'pipeline'})[0], 429)
