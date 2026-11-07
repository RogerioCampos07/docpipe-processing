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
isolated PostgreSQL and RabbitMQ 4.3.6 services and enforces 80% coverage. Tests
use synthetic Processing-owned data and never import another service or require
its runtime. RabbitMQ integration tests also require `PROCESSING_AMQP_URL` for a
disposable vhost; the broker-restart proof uses
`PROCESSING_RABBITMQ_CONTAINER_ID` to restart only that disposable container.
Never point these tests at a shared or operational broker.

The destructive PostgreSQL recovery tests are separate from normal CI and
require `PROCESSING_POSTGRES_CONTAINER_ID` for a disposable container named
`docpipe-et6-pg-*`, running `postgres:17.11-bookworm`. They stop and restart
that exact container. Do not launch it with Docker's `--rm` option: the tests
need to start the same container after stopping it. Use a fixed loopback port
(for example, `127.0.0.1:55432:5432`) and configure
`PROCESSING_TEST_DATABASE_URL` with that port. The test checks that the
container's mapped port remains stable across restart. The database supplied through
`PROCESSING_TEST_DATABASE_URL` must end in `_test`; each test uses its own
migrated schema. The tests verify startup while PostgreSQL is down, a prolonged
outage across a worker restart, and interruption after a test-local PostgreSQL
trigger observes an active job insert. They confirm the event remains in the
Processing queue, does not reach the DLQ, and is accepted once after database
recovery. The trigger exists only in the fixture's random schema.

Run these checks only against explicitly disposable PostgreSQL and RabbitMQ
containers. Start a local test stack and export its **container IDs**, not IDs
from `docker ps` chosen by image alone. The PostgreSQL container must be named
`docpipe-et6-pg-*`, use the pinned image, and be created without `--rm`:

```bash
PROCESSING_TEST_DATABASE_URL='postgresql+psycopg://<test-user>:<encoded-password>@127.0.0.1:55432/processing_test' \
PROCESSING_AMQP_URL='amqp://<test-user>:<encoded-password>@127.0.0.1:<amqp-port>/processing_test' \
PROCESSING_POSTGRES_CONTAINER_ID='<docpipe-et6-pg-container-id>' \
PROCESSING_RABBITMQ_CONTAINER_ID='<disposable-rabbitmq-container-id>' \
uv run pytest -m resilience
```

`test_rabbitmq_recovery.py` restarts the named broker and rereads the host port
published by Docker after restart. RabbitMQ 4.3.6 can take over 60 seconds to
recover quorum queues, so the restart proof allows up to 120 seconds for broker
readiness. These tests run manually because they stop service containers; the
regular `tests` CI job uses disposable services but does not interrupt
PostgreSQL.

All RabbitMQ recovery observations use one node. A quorum queue on a single
node is not highly available; evidence covers only persistent messages and
recovery scenarios actually exercised with the container and its data intact.
Loss of the container's data volume, permanent node loss, cluster behavior,
and recovery after administrative deletion are not covered.

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

Run the PostgreSQL/RabbitMQ acceptance integration and broker recovery proofs
with both test-only services configured:

```bash
PROCESSING_TEST_DATABASE_URL='postgresql+psycopg://<test-user>:<encoded-password>@127.0.0.1:5432/processing_test' \
PROCESSING_AMQP_URL='amqp://<test-user>:<encoded-password>@127.0.0.1:5672/processing_test' \
PROCESSING_RABBITMQ_CONTAINER_ID='<disposable-container-id>' \
uv run pytest tests/test_consumer_integration.py tests/test_rabbitmq_recovery.py
```

`test_consumer_integration.py` checks durable acceptance, equivalent redelivery,
conflict quarantine and recovery when the Processing-owned DLQ is missing before
rejection. `test_rabbitmq_recovery.py` checks 26 requeues with delivery-limit
disabled, retention while the DLQ is full, and persistent source/DLQ messages
after broker restart. These are single-node container experiments; they do not
prove cluster availability or protect against administrative deletion races.

The `container-smoke` CI job migrates a disposable PostgreSQL database, starts
API and worker as separate containers, publishes a synthetic event to RabbitMQ,
and queries the accepted job over HTTP. It checks the public response, worker
and API metrics, liveness and unprivileged runtime. No HTTP seed endpoint or
other microservice is involved.
