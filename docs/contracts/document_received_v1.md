# `document.received.v1`

This is the public, versioned input contract implemented by DocPipe Processing.
Any producer may publish a conforming JSON payload. The Processing model and
tests are its own implementation; producers do not share Python code with this
repository.

## Identity and version

- Logical contract: `document.received.v1`
- `event_type`: exactly `document.received`
- `event_version`: JSON integer `1`

The logical name is represented in the payload by the pair
`event_type` / `event_version`; it is not an additional payload field.
Processing rejects another event type or version. Incompatible changes require
a new contract version.

## Payload

The payload is a UTF-8 JSON object with this shape:

```json
{
  "event_id": "evt-synthetic-0001",
  "event_type": "document.received",
  "event_version": 1,
  "occurred_at": "2026-01-01T12:00:00.123456Z",
  "document_id": "doc-synthetic-0001",
  "data": {
    "storage_key": "opaque-object-key-0001",
    "media_type": "application/pdf; charset=binary",
    "size_bytes": 0,
    "sha256": "0000000000000000000000000000000000000000000000000000000000000000"
  }
}
```

`correlation_id` may be omitted, set to `null`, or set to a non-empty string.
The `correlation_id: null` form is also represented in the fixture at
`tests/fixtures/document_received_v1.json`.

## Fields

| Field | Type and rule |
| --- | --- |
| `event_id` | Required, non-empty string. Opaque identity of the event; conforming producers must keep it globally unique. Processing does not check global uniqueness in the schema. |
| `event_type` | Required literal `document.received`. |
| `event_version` | Required integer literal `1`. |
| `occurred_at` | Required RFC 3339 timestamp with an explicit timezone. Up to six fractional digits are accepted; Processing normalizes the parsed value to UTC. |
| `correlation_id` | Optional nullable string. If supplied as a string, it must not be empty or whitespace only. |
| `document_id` | Required, non-empty opaque string identifying the document. It is distinct from `event_id` and the Processing-owned `processing_id`. |
| `data.storage_key` | Required, non-empty opaque object reference. Producers must not put a public URL or credentials here. Processing does not interpret or open the value. |
| `data.media_type` | Required syntactically valid MIME type, including valid optional parameters. Contract validity does not promise that a future processing pipeline supports that type. |
| `data.size_bytes` | Required JSON integer greater than or equal to zero. Zero is structurally valid and does not promise processability. |
| `data.sha256` | Required 64-character hexadecimal SHA-256. Uppercase and lowercase are accepted; Processing stores the typed value in lowercase. |

Unknown fields in the envelope and `data` are ignored. Known fields remain
strictly validated. Optional additive fields may be introduced only when
older consumers can safely ignore them; changing a required field, a type, or
its meaning requires a new version.

The event is separate from Processing's internal `ProcessingJob`. A future
application layer may map `event_id` to `source_event_id`, `document_id` to
`document_id`, and `correlation_id` to `correlation_id`. Contract validation
does not create or persist a job.

W3C Trace Context belongs to transport headers and is not part of this JSON
payload. This contract has no broker-specific encoding or behavior.

## Contract tests

Run the synthetic, infrastructure-free contract tests with:

```bash
uv run pytest -m contract
```
