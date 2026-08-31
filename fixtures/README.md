# Conformance fixtures

One file per case. Each fixture is a literal request/response pair against protocol wire major
v1: a server implementation must produce the given response for the given request, and an SDK
must produce requests of the given shape and react to the response as the spec's error table
says.

Shape:

```json
{
  "name": "…",
  "description": "…",
  "request":  { "method": "POST", "path": "/ingest/v1", "headers": { }, "body": { } },
  "response": { "status": 200, "body": { } }
}
```

`Authorization` uses the placeholder token `ck_test_valid`; servers under test treat exactly that
string as a valid workspace token and everything else as invalid. Dynamic values (`accepted`
counts, `supported_majors`) are literal in the fixture and normative.
