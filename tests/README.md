# Test organization

The current suite covers HTTP health, operational logs and metrics, the domain,
Processing-owned persistence, and the public input contract. HTTP tests use
FastAPI's in-process client; persistence tests use temporary SQLite databases
and this repository's migrations; contract tests use local synthetic payloads
and fixtures. These tests require no other microservice or external service.

Tests must preserve the [service autonomy decision](../docs/DESIGN.md), section
2.1: no imports, private fixtures, database access, checkout or runtime of another
microservice. Future integration tests may require Processing's infrastructure
(broker or object storage), using compatible synthetic inputs. Tests involving
multiple DocPipe services may complement, but must not replace or become a
prerequisite for, the service's independent suite and CI.

Pytest markers reserve categories for tests added alongside their
implementation: `unit`, `integration`, `contract`, `persistence`, `consumer`,
`storage`, and `e2e`. Do not add placeholder tests for capabilities that do
not exist yet. Keep fixtures synthetic and avoid including document contents
in logs or test output.
