# Cliff Ingest Protocol

**Spec version: 0.0.3** · **Wire major: v1** · Status: pre-release (0.x: anything may change)

The push contract for custom signal integrations into Cliff. Every official SDK, in every
language, is a thin client of this document. The protocol is deliberately small: a bearer token,
a signal name, and rows. Everything else (schema, health, retention) is derived server-side from
the data itself. Two optional surfaces ride beside row ingest: **episodes** (declaring the runs
of an episodic signal) and **labels** (annotation written by connector agents).

## Design invariants

These hold across all future versions of the protocol:

1. **Identity is token + signal name.** The token scopes the caller to a workspace. The signal
   name is the signal's identity within that workspace. Signals are auto-created on first write;
   there is no registration step and no site/device topology.
2. **Rows teach the schema.** Clients never declare types, units, or structure. The server infers
   the shape from the rows and versions it internally. A client that starts sending a new field
   has, by that act alone, added the field.
3. **At-least-once delivery, idempotent by `batch_id`.** A client retries a failed request with
   the same `batch_id`; the server deduplicates. Sending the same rows under a new `batch_id` is
   new data.
4. **Liveness is derived from cadence.** There are no lifecycle messages (no births, deaths, or
   heartbeats). A signal that stops arriving goes stalled by observation, the same way polled
   signals do.

## Versioning

- The **wire major** is in the URL path (`/ingest/v1`). Within a major, changes are additive
  only: new optional request fields, new response fields, new error codes. Clients MUST ignore
  unknown fields in responses; servers MUST ignore unknown fields in requests.
- The **spec version** (this document, currently 0.0.1) is semver over the prose and fixtures.
  While the spec is 0.x, the wire contract may still change incompatibly without a major bump.
  From 1.0.0 onward, an incompatible change means a new path major, and servers answer requests
  for retired majors with `unsupported_version` naming the majors they support.
- **SDK versions are independent of both.** Each SDK versions freely per language and declares
  the spec revision it implements. Clients SHOULD send `X-Cliff-SDK` (`<language>/<sdk version>`,
  e.g. `python/0.0.1`) and `X-Cliff-Protocol` (the spec revision, e.g. `0.0.1`) on every
  request, so the deployed client population is observable before any change ships.

## Request

```
POST {endpoint}/ingest/v1
Authorization: Bearer <token>
Content-Type: application/json
X-Cliff-SDK: python/0.0.1
X-Cliff-Protocol: 0.0.1
```

```json
{
  "batch_id": "0f8fad5b-d9cb-469f-a165-70867728950e",
  "sent_at": "2026-08-30T18:04:11.201Z",
  "batches": [
    {
      "signal": "arm-1",
      "rows": [
        { "time": "2026-08-30T18:04:10.950Z",
          "joints": { "j1": { "pos": 1.21, "torque": 0.4 } },
          "gripper": 0.8 }
      ]
    }
  ]
}
```

### Envelope fields

| field      | type   | required | meaning |
|------------|--------|----------|---------|
| `batch_id` | string (UUID) | yes | Idempotency key for this request. Reused verbatim on retry. |
| `sent_at`  | string (RFC 3339) | no | Client wall-clock at send time. Used only to observe clock skew and queue delay; never used as a sample timestamp. |
| `batches`  | array  | yes | One entry per signal. A request MAY carry many signals; a signal MUST NOT appear twice in one request. |

### Batch fields

| field    | type   | required | meaning |
|----------|--------|----------|---------|
| `signal` | string | yes | Signal name. 1–200 characters after trimming; any printable Unicode. Auto-creates the signal on first write. |
| `rows`   | array of objects | yes | The samples, oldest first within the batch. |

### Rows

- A row is a JSON object. Nesting is preserved: `{"meta":{"line":3}}` is the field path
  `meta.line`. Arrays are kept whole and indexed downstream; they are not exploded into
  per-index fields.
- `time` is the reserved axis key. It is either an RFC 3339 string or an integer of epoch
  **milliseconds**. If absent, the server stamps the row with receive time. All other keys are
  data.
- `null` values are permitted and say nothing about a field's type.
- Everything else about a row is the integration's business. The protocol imposes no schema.

### Limits (provisional at 0.0.1)

- Request body: ≤ 1 MiB.
- Rows per request (across all batches): ≤ 5,000.
- Requests exceeding either limit are rejected whole with `too_large` (HTTP 413).

## Response

The request is atomic: it is accepted whole or rejected whole. There are no partial successes,
which is what keeps every client's retry logic trivial.

**200 OK**

```json
{ "accepted": 412, "duplicate": false }
```

- `accepted`: rows accepted in this request.
- `duplicate`: `true` when `batch_id` was already seen; the request was a no-op and `accepted`
  reports `0`. The dedupe window is at least 1 hour (provisional).

**Errors**

```json
{ "error": { "code": "unsupported_version", "message": "v1 is retired", "supported_majors": [2] } }
```

| HTTP | `code` | client action |
|------|--------|---------------|
| 400  | `malformed` | Do not retry. Surface to the caller: the request body is invalid (bad JSON, missing field, duplicate signal in one request, bad `time`). |
| 401  | `unauthorized` | Do not retry. The token is missing, wrong, or revoked. |
| 404  | `unsupported_version` | Do not retry. `supported_majors` names what the server speaks. |
| 413  | `too_large` | Do not retry the same body. Split and resend as smaller requests with fresh `batch_id`s. |
| 429  | `rate_limited` | Retry with the same `batch_id` after `Retry-After` seconds. |
| 5xx  | `internal` | Retry with the same `batch_id`, jittered exponential backoff. |

## Episodes (since 0.0.2)

Some signals are runs, not streams: a test fires, a batch executes, an arm attempts a task.
Episodes let the writer declare that structure explicitly — the server never infers a run.

**Open** — `POST {endpoint}/ingest/v1/episodes`

```json
{ "signal": "test-rig-7", "id": "<uuid, optional>", "meta": {"build": "rev-14"}, "opened_at": "<RFC 3339, optional>" }
```

→ `{ "episode": "<uuid>", "opened_at": "…", "closed": false }`

- `id` is client-minted for at-least-once safety: a retried open with the same id is the SAME
  episode, and the response reports that episode's stored facts, not the request's. Omit it and
  the server mints one (then a retried open is a twin — supply the id).
- `opened_at` defaults to now; a harness that knows the run started earlier says so.
- Opening an episode marks the signal episodic and auto-creates it like row ingest does.

**Close** — `POST {endpoint}/ingest/v1/episodes/{id}/close` with optional `{"closed_at": "…"}`.
Idempotent: the first close wins and a retry never moves history. The server never closes an
episode itself — a crashed writer leaves a visibly stale open episode for a human, and the
system does not guess that a run ended.

**Attribution** — the row envelope takes an optional top-level `"episode": "<uuid>"`, applying
to every row in that request (attribution is batch-granular; split requests to split
attribution). The episode must exist for the tenant (`malformed` otherwise). Batches for a
CLOSED episode are accepted: late data lands, history repair is sacred, nothing re-triggers.

## Labels (since 0.0.3)

Annotation over signals' time: the work order a CMMS connector syncs, the defect a quality
agent files. Same token, same error envelope — a connector is an integration like any other.

**Write** — `POST {endpoint}/ingest/v1/labels`

```json
{
  "labels": [
    {
      "start": "2026-09-01T05:10:00Z",
      "end": "2026-09-01T05:42:00Z",
      "subjects": [ { "signal": "arm-1" } ],
      "metadata": { "source": "cliff-cmms", "kind": "work-order", "ref": "wo:4812" },
      "properties": { "status": "closed", "cause": "bearing wear" }
    }
  ]
}
```

→ `{ "accepted": 1 }`

- **`metadata.source` and `metadata.ref` are REQUIRED on this endpoint** and together are the
  label's identity: writing the same (source, ref) again revises the label in place. This is
  the whole idempotency story — no batch id, no ledger. A connector re-sync is always safe, and
  a connector that cannot supply a stable ref should not be writing labels.
- `subjects` are signal **names** (resolved and auto-created like row ingest), so an
  integration never needs a UUID. `metadata.kind` names what the label is (`work-order`,
  `defect`); everything else in `metadata` and all of `properties` is the producer's business —
  the rows teach the per-kind shape.
- `end` absent = still open (a work order not yet closed); `end` equal to `start` =
  instantaneous. At most 500 labels per request; the request is accepted or rejected whole.

Reading labels back (range intersection, metadata filters) is the workspace API's job, not this
protocol's: integrations write.

## Client requirements

An official SDK MUST:

- batch rows client-side and flush on an interval (default 1s) or when a size threshold is hit;
- bound its buffer and expose an explicit overflow policy (block the producer, or drop oldest);
  overflow is never silent;
- stamp `time` at capture, not at send, so queue delay never skews the axis;
- retry retryable failures with the same `batch_id` and jittered exponential backoff;
- flush on close, and never lose rows on a clean shutdown.

The conformance fixtures in [`fixtures/`](fixtures/) are request/response pairs every server and
SDK implementation is tested against.

## Explicitly out of scope

Schema or type declaration, site/device hierarchy, connection lifecycle semantics, compression
negotiation, and transports other than HTTP. A streaming (gRPC) binding of the same logical
contract may be added later as its own document; it will not change this one.

## Changelog

- **0.0.3** (2026-09-01): labels — `POST /ingest/v1/labels`, identity-required annotation
  writes for connector agents.
- **0.0.2** (2026-08-31): episodes — open/close lifecycle and the envelope's `episode`
  attribution. (Documented after shipping; the spec now leads again.)
- **0.0.1** (2026-08-30): initial draft.
