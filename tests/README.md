# Test organization

Domain, public contract, configuration and HTTP tests using fakes require no
external service:

```bash
uv run pytest -m 'not persistence'
```

Persistence tests require real PostgreSQL 17.11 and
`PROCESSING_TEST_DATABASE_URL`, using `postgresql+psycopg` and a database name
ending in `_test`. Missing configuration fails rather than silently skipping.
The test role needs CREATE SCHEMA on that dedicated database. Each test creates
a random schema, applies this repository's migrations and drops only its own
schema. There is no outer transaction hiding repository commits. Migration,
legacy compatibility, constraints, concurrency, rollback and engine restart
are exercised against PostgreSQL. No SQLite fallback is supported.

```bash
uv run pytest -m persistence
uv run task test
```

The complete suite requires the same test database configuration. CI provides
an isolated PostgreSQL service and enforces 80% coverage. Tests use synthetic
Processing-owned data and never import another service or require its runtime.
No RabbitMQ consumer, storage retrieval or document pipeline is implemented.
Future integration tests will extend the suite alongside those capabilities.

`test_application.py` covers the transport-independent query with a minimal
reader fake. `test_api.py` checks the HTTP projection, safe failure messages,
UUID validation, errors, OpenAPI, lifecycle and operational observability.
`test_api_persistence.py` joins the actual API composition and repository with
isolated PostgreSQL schemas. It verifies committed snapshots, all states, legacy
rows, unchanged input and safe errors for inconsistent data/schema.

Run the domain HTTP and OpenAPI tests without a database:

```bash
uv run pytest tests/test_application.py tests/test_api.py
```

With the dedicated PostgreSQL test configuration above:

```bash
uv run pytest tests/test_api_persistence.py
```

The existing `container-smoke` CI job migrates its disposable PostgreSQL database
from the runtime image, creates a synthetic job and durable input through the
domain/repository, then queries it over HTTP. It checks the exact public fields
and retains health, metrics and unprivileged-runtime checks. No HTTP seed
endpoint, broker or other microservice is involved.
