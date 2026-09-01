# cliff-sdk (Python)

Push signals into Cliff. Zero dependencies, Python ≥ 3.9.

```bash
pip install cliff-sdk
```

```python
import cliff_sdk

c = cliff_sdk.connect()                    # CLIFF_TOKEN / CLIFF_ENDPOINT, or kwargs
arm = c.signal("arm-1")
arm.emit({"joints": {"j1": {"pos": 1.21, "torque": 0.4}}, "gripper": 0.8})
c.close()                                  # flushes; also runs automatically at exit
```

No schema, no registration: the first `emit` creates the signal, and the rows teach Cliff the
shape. The client batches in the background (1s flush by default), stamps capture time at
`emit`, bounds its buffer with an explicit overflow policy (`overflow="block"` or
`"drop_oldest"`), and retries with the same batch id so a retry can never double-count.

## Versions

This SDK versions independently of the protocol and declares what it implements:

| constant | value | meaning |
|---|---|---|
| `cliff_sdk.SDK_VERSION` | 0.0.3 | this client |
| `cliff_sdk.PROTOCOL_VERSION` | 0.0.3 | the spec revision implemented ([`PROTOCOL.md`](../PROTOCOL.md)) |
| `cliff_sdk.WIRE_MAJOR` | 1 | the `/ingest/v{N}` path major spoken |

## Episodes and labels

```python
with c.open_episode("test-rig-7", meta={"build": "rev-14"}) as ep:
    c.signal("test-rig-7").emit({"thrust_n": 412.5}, episode=ep)
# closed on exit; opens are idempotent by client-minted id, first close wins

c.put_label(start="2026-09-01T05:10:00Z", end="2026-09-01T05:42:00Z",
            signal="arm-1", source="cliff-cmms", kind="work-order", ref="wo:4812",
            properties={"status": "closed", "cause": "bearing wear"})
# idempotent by (source, ref): re-syncing a revised record is always safe
```

Tests: `python -m unittest discover -s tests` (stdlib only).
