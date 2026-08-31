# cliff-sdk

The ingest protocol for custom signal integrations into [Cliff](https://trycliff.xyz), and the
official SDKs for it: one directory per language, all thin clients of one wire contract.

**Status: pre-release.** The contract is deliberately tiny: a bearer token, a signal name, and
rows of JSON. Signals are auto-created on first write, the schema is inferred from the rows
server-side, delivery is at-least-once with idempotent batches, and liveness is derived from
cadence. There is nothing to register and nothing to declare.

```python
import cliff_sdk

c = cliff_sdk.connect(token="ck_…", endpoint="https://…")
arm = c.signal("arm-1")
arm.emit({"joints": {"j1": {"pos": 1.21, "torque": 0.4}}, "gripper": 0.8})
```

## Layout

- [`PROTOCOL.md`](PROTOCOL.md): the wire contract, currently **0.0.1**. This is the primary
  artifact; every SDK is a thin client of it.
- [`fixtures/`](fixtures/): conformance fixtures, literal request/response pairs that both
  server and SDK implementations are tested against.
- [`python/`](python/): the Python SDK (`pip install cliff-sdk`, `import cliff_sdk`). Zero
  dependencies. More languages land as sibling directories.

## Versioning

Three versions, deliberately independent:

- **Wire major** in the URL path (`/ingest/v1`): the only version the server routes on.
- **Protocol spec** (semver over `PROTOCOL.md` + fixtures, currently 0.0.1): additive within a
  wire major; while 0.x anything may change.
- **Each SDK** versions freely per language and declares the spec revision it implements (a
  constant, a README line, and the `X-Cliff-SDK` / `X-Cliff-Protocol` headers on every request),
  so an SDK can fix bugs or rework its own API without pretending the protocol changed.

Tags in this repo are namespaced per component: `protocol/v0.0.1`, `python/v0.0.1`.

## License

MIT.
