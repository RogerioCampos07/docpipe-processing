# Test organization

The current suite covers the implemented HTTP health endpoint, its operational
log, and the metrics endpoint. It uses FastAPI's in-process test client and
does not require external services.

Pytest markers reserve categories for tests added alongside their
implementation: `unit`, `integration`, `contract`, `persistence`, `consumer`,
`storage`, and `e2e`. Do not add placeholder tests for capabilities that do
not exist yet. Keep fixtures synthetic and avoid including document contents
in logs or test output.
