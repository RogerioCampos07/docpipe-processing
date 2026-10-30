# Test organization

Domain, public contract, configuration and HTTP tests require no external service:

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
