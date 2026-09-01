"""A thin client of PROTOCOL.md, wire major v1.

The whole job of this module is the boring, load-bearing part of ingest: background batching,
a bounded buffer with an explicit overflow policy, capture-time stamping, and retries that reuse
the batch id so the server can deduplicate. Episodes and labels ride beside row ingest as small
synchronous calls — an episode open is idempotent by client-minted id, and a label write is
idempotent by (source, ref), so every retry here is safe by construction. There is deliberately
nothing else in here: no schema, no registration, no domain semantics. Rows teach the server
everything.
"""

from __future__ import annotations

import atexit
import json
import os
import random
import sys
import threading
import time as _time
import urllib.error
import urllib.request
import uuid
from collections import deque
from datetime import datetime, timezone
from typing import Callable, Optional, Union

SDK_VERSION = "0.0.3"  # this client's own version; moves freely
PROTOCOL_VERSION = "0.0.3"  # the spec revision this client implements
WIRE_MAJOR = 1  # the /ingest/v{N} path major

__all__ = [
    "connect",
    "Client",
    "Signal",
    "Episode",
    "IngestError",
    "SDK_VERSION",
    "PROTOCOL_VERSION",
    "WIRE_MAJOR",
]

# Statuses we retry (rows with the same batch_id, episodes by client id, labels by source+ref —
# in every case a retry can never double-count).
_RETRYABLE = {429, 500, 502, 503, 504}
_BACKOFF_CAP = 30.0
_SYNC_ATTEMPTS = 5  # synchronous calls (episodes, labels) raise after this many tries

# The spec caps a request at 1 MiB; we chunk flushes to stay under it with room for the
# envelope, because a server (or the load balancer in front of it) may hang up mid-upload on an
# oversized body — which surfaces as a connection error, not a tidy 413. Learned from a 60-minute
# backfill whose 5000-row batches of nested rows weighed ~1.5 MiB each.
_BODY_BUDGET = 900_000


class IngestError(Exception):
    """Rows were permanently lost, or a synchronous call was permanently refused.

    For rows this is delivered to the client's ``on_error`` hook rather than raised, because
    the producer's thread has usually moved on by the time the failure is known. Episode and
    label calls raise it directly — their callers are still there and want the answer.
    """

    def __init__(self, code: str, message: str, rows_lost: int = 0):
        super().__init__(f"{code}: {message}" + (f" ({rows_lost} rows lost)" if rows_lost else ""))
        self.code = code
        self.message = message
        self.rows_lost = rows_lost


def connect(
    token: Optional[str] = None,
    endpoint: Optional[str] = None,
    **opts,
) -> "Client":
    """Create a client from arguments or the environment (CLIFF_TOKEN, CLIFF_ENDPOINT)."""
    token = token or os.environ.get("CLIFF_TOKEN")
    endpoint = endpoint or os.environ.get("CLIFF_ENDPOINT")
    if not token:
        raise ValueError("no token: pass connect(token=...) or set CLIFF_TOKEN")
    if not endpoint:
        raise ValueError("no endpoint: pass connect(endpoint=...) or set CLIFF_ENDPOINT")
    return Client(token, endpoint, **opts)


class Client:
    """Owns the buffer and the background flusher. One per process is the normal shape."""

    def __init__(
        self,
        token: str,
        endpoint: str,
        *,
        flush_interval: float = 1.0,
        max_batch_rows: int = 5000,
        max_buffer_rows: int = 100_000,
        overflow: str = "block",
        timeout: float = 10.0,
        on_error: Optional[Callable[[IngestError], None]] = None,
    ):
        if overflow not in ("block", "drop_oldest"):
            raise ValueError("overflow must be 'block' or 'drop_oldest'")
        self._token = token
        self._url = endpoint.rstrip("/") + f"/ingest/v{WIRE_MAJOR}"
        self._flush_interval = flush_interval
        self._max_batch_rows = max_batch_rows
        self._max_buffer_rows = max_buffer_rows
        self._overflow = overflow
        self._timeout = timeout
        self._on_error = on_error

        self._buf: deque = deque()  # of (signal_name, row, episode_id | None)
        self._cond = threading.Condition()
        self._closing = False
        self._dropped_since_flush = 0

        self._thread = threading.Thread(
            target=self._run, name="cliff-sdk-flusher", daemon=True
        )
        self._thread.start()
        atexit.register(self.close)

    # ------------------------------------------------------------------------------ producer side

    def signal(self, name: str) -> "Signal":
        name = name.strip()
        if not (1 <= len(name) <= 200):
            raise ValueError("signal name must be 1-200 characters")
        return Signal(self, name)

    def open_episode(
        self,
        signal: str,
        *,
        meta: Optional[dict] = None,
        opened_at: Union[None, str, datetime] = None,
    ) -> "Episode":
        """Declare a run starting on `signal`. Synchronous and idempotent: the id is minted
        here, so a retried open is the same episode, never a twin. Usable as a context
        manager — the episode closes on exit."""
        body: dict = {"signal": signal, "id": str(uuid.uuid4())}
        if meta is not None:
            body["meta"] = meta
        if opened_at is not None:
            body["opened_at"] = _stamp_str(opened_at, "opened_at")
        resp = self._call("/episodes", body)
        return Episode(self, resp["episode"])

    def put_label(
        self,
        *,
        start: Union[str, datetime],
        end: Union[None, str, datetime] = None,
        signal: Union[str, list],
        source: str,
        kind: str,
        ref: str,
        properties: Optional[dict] = None,
        metadata: Optional[dict] = None,
    ) -> None:
        """Write one label. Idempotent by (source, ref): re-syncing a revised record is always
        safe, which is the contract a connector needs. Raises IngestError on refusal."""
        signals = signal if isinstance(signal, list) else [signal]
        md = dict(metadata or {})
        md.update({"source": source, "kind": kind, "ref": ref})
        label = {
            "start": _stamp_str(start, "start"),
            "subjects": [{"signal": s} for s in signals],
            "metadata": md,
            "properties": properties or {},
        }
        if end is not None:
            label["end"] = _stamp_str(end, "end")
        self.put_labels([label])

    def put_labels(self, labels: list) -> int:
        """Write labels in bulk, PROTOCOL.md shapes verbatim. Returns the accepted count."""
        resp = self._call("/labels", {"labels": labels})
        return int(resp.get("accepted", len(labels)))

    def _enqueue(
        self,
        signal: str,
        row: dict,
        time: Union[None, str, int, datetime],
        episode: Union[None, str, "Episode"],
    ) -> None:
        if not isinstance(row, dict):
            raise TypeError("a row is a dict")
        row = dict(row)
        if "time" not in row:
            row["time"] = _stamp(time)
        elif time is not None:
            raise ValueError("row already has 'time'; don't also pass time=")
        ep = episode.id if isinstance(episode, Episode) else episode
        with self._cond:
            if self._closing:
                raise RuntimeError("client is closed")
            if self._overflow == "block":
                while len(self._buf) >= self._max_buffer_rows and not self._closing:
                    self._cond.wait()
                if self._closing:
                    raise RuntimeError("client is closed")
            else:  # drop_oldest
                while len(self._buf) >= self._max_buffer_rows:
                    self._buf.popleft()
                    self._dropped_since_flush += 1
            self._buf.append((signal, row, ep))
            if len(self._buf) >= self._max_batch_rows:
                self._cond.notify_all()  # wake the flusher early

    # ------------------------------------------------------------------------------- flusher side

    def _run(self) -> None:
        while True:
            with self._cond:
                deadline = _time.monotonic() + self._flush_interval
                while not self._closing and len(self._buf) < self._max_batch_rows:
                    remaining = deadline - _time.monotonic()
                    if remaining <= 0:
                        break
                    self._cond.wait(remaining)
                closing = self._closing
            self._flush_once()
            if closing:
                with self._cond:
                    if not self._buf:
                        return

    def _flush_once(self) -> None:
        with self._cond:
            if self._dropped_since_flush:
                dropped, self._dropped_since_flush = self._dropped_since_flush, 0
            else:
                dropped = 0
            rows = list(self._buf)
            self._buf.clear()
            self._cond.notify_all()  # blocked producers may proceed
        if dropped:
            self._report(IngestError("overflow", "buffer full, oldest rows dropped", dropped))
        if not rows:
            return

        # Split into requests bounded by row count, encoded size, AND episode — attribution is
        # envelope-granular, so rows for different episodes never share a request. Each chunk is
        # its own request with its own batch_id; a retry replays a chunk, never the whole flush.
        chunk: list = []
        size = 0
        chunk_ep: Optional[str] = None
        for name, row, ep in rows:
            cost = len(json.dumps(row, separators=(",", ":")).encode()) + len(name.encode()) + 32
            split = chunk and (
                len(chunk) >= self._max_batch_rows or size + cost > _BODY_BUDGET or ep != chunk_ep
            )
            if split:
                self._send_chunk(chunk, chunk_ep)
                chunk, size = [], 0
            chunk_ep = ep
            chunk.append((name, row))
            size += cost
        if chunk:
            self._send_chunk(chunk, chunk_ep)

    def _send_chunk(self, rows: list, episode: Optional[str]) -> None:
        # Group by signal, preserving per-signal order. Order across signals is irrelevant.
        by_signal: dict = {}
        for name, row in rows:
            by_signal.setdefault(name, []).append(row)
        body = {
            "batch_id": str(uuid.uuid4()),  # one id per chunk, reused verbatim across retries
            "sent_at": _now_rfc3339(),
            "batches": [{"signal": s, "rows": rs} for s, rs in by_signal.items()],
        }
        if episode is not None:
            body["episode"] = episode
        self._send(body, n_rows=len(rows))

    def _send(self, body: dict, n_rows: int) -> None:
        payload = json.dumps(body, separators=(",", ":")).encode()
        attempt = 0
        while True:
            status, resp_body, retry_after = self._post(self._url, payload)
            if status == 200:
                return
            retryable = status in _RETRYABLE or status == 0  # 0 = network error
            if retryable and not (self._closing and attempt >= 2):
                delay = retry_after or min(_BACKOFF_CAP, 0.5 * (2**attempt)) * random.uniform(0.5, 1.0)
                attempt += 1
                _time.sleep(delay)
                continue
            code, message = _parse_error(status, resp_body)
            self._report(IngestError(code, message, n_rows))
            return

    # -------------------------------------------------------------------------- synchronous calls

    def _call(self, path: str, body: dict) -> dict:
        """Episodes and labels: bounded retries, then raise. Callers are present and want the
        answer, unlike the fire-and-forget row path."""
        payload = json.dumps(body, separators=(",", ":")).encode()
        for attempt in range(_SYNC_ATTEMPTS):
            status, resp_body, retry_after = self._post(self._url + path, payload)
            if status == 200:
                try:
                    return json.loads(resp_body)
                except Exception:
                    return {}
            if status in _RETRYABLE or status == 0:
                if attempt + 1 < _SYNC_ATTEMPTS:
                    delay = retry_after or min(_BACKOFF_CAP, 0.5 * (2**attempt)) * random.uniform(0.5, 1.0)
                    _time.sleep(delay)
                    continue
            raise IngestError(*_parse_error(status, resp_body))
        raise IngestError("unreachable", f"gave up after {_SYNC_ATTEMPTS} attempts")

    def _post(self, url: str, payload: bytes):
        """One HTTP attempt: (status, body, retry_after). Status 0 means the wire failed."""
        req = urllib.request.Request(
            url,
            data=payload,
            headers={
                "Authorization": f"Bearer {self._token}",
                "Content-Type": "application/json",
                "X-Cliff-SDK": f"python/{SDK_VERSION}",
                "X-Cliff-Protocol": PROTOCOL_VERSION,
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                return resp.status, resp.read(), None
        except urllib.error.HTTPError as e:
            retry_after = None
            if e.code == 429:
                try:
                    retry_after = float(e.headers.get("Retry-After", ""))
                except ValueError:
                    pass
            return e.code, e.read(), retry_after
        except Exception:
            return 0, b"", None

    def _report(self, err: IngestError) -> None:
        if self._on_error is not None:
            try:
                self._on_error(err)
            except Exception:
                pass
        else:
            sys.stderr.write(f"cliff-sdk: {err}\n")

    # ------------------------------------------------------------------------------------ closing

    def close(self, timeout: Optional[float] = 10.0) -> None:
        """Flush what's buffered and stop. Safe to call twice; called at exit automatically."""
        with self._cond:
            self._closing = True
            self._cond.notify_all()
        self._thread.join(timeout)

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


class Signal:
    """A name bound to a client. Cheap; make as many as you like."""

    def __init__(self, client: Client, name: str):
        self._client = client
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    def emit(
        self,
        row: dict,
        time: Union[None, str, int, datetime] = None,
        episode: Union[None, str, "Episode"] = None,
    ) -> None:
        """Queue one sample. Stamped with capture time now unless `time` (or row['time']) says
        otherwise. Pass `episode` to attribute the row to an open (or closed — late data is
        welcome) episode."""
        self._client._enqueue(self._name, row, time, episode)


class Episode:
    """A declared run. Close it explicitly, or use it as a context manager."""

    def __init__(self, client: Client, id: str):
        self._client = client
        self.id = id

    def close(self, closed_at: Union[None, str, datetime] = None) -> None:
        """Idempotent: the first close wins; a retry never moves history."""
        body = {}
        if closed_at is not None:
            body["closed_at"] = _stamp_str(closed_at, "closed_at")
        self._client._call(f"/episodes/{self.id}/close", body)

    def __enter__(self) -> "Episode":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


def _now_rfc3339() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _stamp(t: Union[None, str, int, datetime]) -> Union[str, int]:
    if t is None:
        return _now_rfc3339()
    if isinstance(t, datetime):
        if t.tzinfo is None:
            raise ValueError("naive datetime: attach a timezone")
        return t.isoformat(timespec="milliseconds")
    if isinstance(t, bool) or not isinstance(t, (str, int)):
        raise TypeError("time must be an RFC 3339 string, epoch milliseconds int, or datetime")
    return t


def _stamp_str(t: Union[str, datetime], name: str) -> str:
    if isinstance(t, datetime):
        if t.tzinfo is None:
            raise ValueError(f"naive datetime for {name}: attach a timezone")
        return t.isoformat(timespec="milliseconds")
    if isinstance(t, str):
        return t
    raise TypeError(f"{name} is an RFC 3339 string or datetime")


def _parse_error(status: int, body: bytes):
    if status == 0:
        return "unreachable", "endpoint unreachable"
    try:
        err = json.loads(body)["error"]
        return err["code"], err["message"]
    except Exception:
        return f"http_{status}", (body or b"")[:200].decode(errors="replace")
