"""Behavioral tests against a scripted local HTTP server. Stdlib only."""

import json
import sys
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from cliff_sdk import Client, IngestError  # noqa: E402


class StubServer:
    """Records every request; answers from a script of (status, body) tuples, then 200s."""

    def __init__(self):
        self.requests = []
        self.script = []
        self.lock = threading.Lock()
        stub = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                length = int(self.headers.get("Content-Length", 0))
                body = json.loads(self.rfile.read(length))
                with stub.lock:
                    stub.requests.append(
                        {"path": self.path, "headers": dict(self.headers), "body": body}
                    )
                    if stub.script:
                        status, resp = stub.script.pop(0)
                    elif self.path.endswith("/episodes"):
                        status, resp = 200, {"episode": body["id"], "opened_at": "x", "closed": False}
                    elif self.path.endswith("/close"):
                        status, resp = 200, {"closed_at": "x"}
                    elif self.path.endswith("/labels"):
                        status, resp = 200, {"accepted": len(body["labels"])}
                    else:
                        status, resp = 200, {"accepted": sum(len(b["rows"]) for b in body["batches"]),
                                             "duplicate": False}
                payload = json.dumps(resp).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *args):
                pass

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()
        self.endpoint = f"http://127.0.0.1:{self.httpd.server_address[1]}"

    def stop(self):
        self.httpd.shutdown()
        self.httpd.server_close()


class Harness(unittest.TestCase):
    def setUp(self):
        self.server = StubServer()
        self.errors = []

    def tearDown(self):
        self.server.stop()

    def client(self, **opts):
        opts.setdefault("flush_interval", 0.05)
        opts.setdefault("on_error", self.errors.append)
        return Client("ck_test_valid", self.server.endpoint, **opts)


class ClientTest(Harness):
    def test_batches_group_by_signal_and_close_flushes(self):
        with self.client() as c:
            arm = c.signal("arm-1")
            cam = c.signal("arm-1-camera")
            arm.emit({"torque": 0.4})
            arm.emit({"torque": 0.5})
            cam.emit({"frame": [1, 2]})
        self.assertEqual(len(self.server.requests), 1)
        req = self.server.requests[0]
        self.assertEqual(req["path"], "/ingest/v1")
        headers = {k.lower(): v for k, v in req["headers"].items()}
        self.assertEqual(headers["x-cliff-sdk"].split("/")[0], "python")
        self.assertEqual(headers["authorization"], "Bearer ck_test_valid")
        batches = {b["signal"]: b["rows"] for b in req["body"]["batches"]}
        self.assertEqual(set(batches), {"arm-1", "arm-1-camera"})
        self.assertEqual([r["torque"] for r in batches["arm-1"]], [0.4, 0.5])
        for rows in batches.values():
            for row in rows:
                self.assertIn("time", row)  # stamped at capture
        self.assertEqual(self.errors, [])

    def test_retry_reuses_batch_id(self):
        self.server.script = [(500, {"error": {"code": "internal", "message": "boom"}})]
        with self.client() as c:
            c.signal("arm-1").emit({"x": 1})
        self.assertEqual(len(self.server.requests), 2)
        ids = [r["body"]["batch_id"] for r in self.server.requests]
        self.assertEqual(ids[0], ids[1])
        self.assertEqual(self.errors, [])

    def test_non_retryable_is_reported_not_retried(self):
        self.server.script = [(401, {"error": {"code": "unauthorized", "message": "bad token"}})]
        with self.client() as c:
            c.signal("arm-1").emit({"x": 1})
        self.assertEqual(len(self.server.requests), 1)
        self.assertEqual(len(self.errors), 1)
        self.assertIsInstance(self.errors[0], IngestError)
        self.assertEqual(self.errors[0].code, "unauthorized")
        self.assertEqual(self.errors[0].rows_lost, 1)

    def test_drop_oldest_overflow_is_reported(self):
        c = self.client(max_buffer_rows=2, overflow="drop_oldest", flush_interval=5)
        s = c.signal("arm-1")
        for i in range(5):
            s.emit({"i": i})
        c.close()
        rows = [r for req in self.server.requests for b in req["body"]["batches"] for r in b["rows"]]
        self.assertEqual([r["i"] for r in rows], [3, 4])
        self.assertEqual(sum(e.rows_lost for e in self.errors), 3)
        self.assertEqual({e.code for e in self.errors}, {"overflow"})

    def test_explicit_time_passthrough(self):
        with self.client() as c:
            c.signal("arm-1").emit({"x": 1}, time=1788717850950)
            c.signal("arm-1").emit({"x": 2, "time": "2026-08-30T18:04:10.950Z"})
        rows = [r for req in self.server.requests for b in req["body"]["batches"] for r in b["rows"]]
        times = {r["x"]: r["time"] for r in rows}
        self.assertEqual(times[1], 1788717850950)
        self.assertEqual(times[2], "2026-08-30T18:04:10.950Z")

    def test_emit_after_close_raises(self):
        c = self.client()
        s = c.signal("arm-1")
        c.close()
        with self.assertRaises(RuntimeError):
            s.emit({"x": 1})

    def test_oversized_flush_splits_by_bytes(self):
        big = "x" * 10_000  # ~10 KB per row; 200 rows ≈ 2 MB, must split under the 1 MiB cap
        c = self.client(flush_interval=5)
        s = c.signal("arm-1")
        for i in range(200):
            s.emit({"i": i, "blob": big})
        c.close()
        self.assertGreater(len(self.server.requests), 1)
        total = 0
        for req in self.server.requests:
            body = json.dumps(req["body"], separators=(",", ":")).encode()
            self.assertLessEqual(len(body), 1 << 20)
            total += sum(len(b["rows"]) for b in req["body"]["batches"])
        self.assertEqual(total, 200)
        ids = [r["body"]["batch_id"] for r in self.server.requests]
        self.assertEqual(len(ids), len(set(ids)))  # each chunk its own batch_id


class EpisodeAndLabelTest(Harness):
    def test_episode_open_is_idempotent_by_client_id(self):
        self.server.script = [(500, {"error": {"code": "internal", "message": "boom"}})]
        c = self.client()
        ep = c.open_episode("test-rig-7", meta={"build": "rev-14"})
        opens = [r for r in self.server.requests if r["path"].endswith("/episodes")]
        self.assertEqual(len(opens), 2)  # the 500 was retried
        self.assertEqual(opens[0]["body"]["id"], opens[1]["body"]["id"])  # same episode, not a twin
        self.assertEqual(ep.id, opens[1]["body"]["id"])
        c.close()

    def test_rows_partition_by_episode(self):
        c = self.client(flush_interval=5)
        s = c.signal("arm-1")
        a = c.open_episode("arm-1")
        b = c.open_episode("arm-1")
        s.emit({"n": 1}, episode=a)
        s.emit({"n": 2}, episode=a)
        s.emit({"n": 3}, episode=b)
        s.emit({"n": 4})  # unattributed
        c.close()
        ingests = [r["body"] for r in self.server.requests if r["path"] == "/ingest/v1"]
        eps = [(body.get("episode"), sum(len(x["rows"]) for x in body["batches"])) for body in ingests]
        self.assertEqual(eps, [(a.id, 2), (b.id, 1), (None, 1)])

    def test_episode_context_manager_closes(self):
        c = self.client()
        with c.open_episode("arm-1") as ep:
            pass
        closes = [r for r in self.server.requests if r["path"].endswith("/close")]
        self.assertEqual(len(closes), 1)
        self.assertIn(ep.id, closes[0]["path"])
        c.close()

    def test_put_label_shape_and_refusal(self):
        c = self.client()
        n = c.put_label(
            start="2026-09-01T05:10:00Z",
            end="2026-09-01T05:42:00Z",
            signal="arm-1",
            source="cliff-cmms",
            kind="work-order",
            ref="wo:4812",
            properties={"status": "closed"},
        )
        req = [r for r in self.server.requests if r["path"].endswith("/labels")][0]
        label = req["body"]["labels"][0]
        self.assertEqual(label["subjects"], [{"signal": "arm-1"}])
        self.assertEqual(
            label["metadata"],
            {"source": "cliff-cmms", "kind": "work-order", "ref": "wo:4812"},
        )
        self.assertEqual(label["properties"], {"status": "closed"})

        self.server.script = [
            (400, {"error": {"code": "malformed", "message": "labels need source+ref"}})
        ]
        with self.assertRaises(IngestError) as caught:
            c.put_labels([{"start": "2026-09-01T05:10:00Z"}])
        self.assertEqual(caught.exception.code, "malformed")
        c.close()


if __name__ == "__main__":
    unittest.main()
