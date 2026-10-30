"""Real PostgreSQL fixtures isolated in schemas owned by each test."""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import make_url
from sqlalchemy.schema import CreateSchema, DropSchema

from docpipe_processing.persistence import create_database_engine

ALEMBIC_CONFIG = Path(__file__).parents[1] / 'alembic.ini'


@dataclass(frozen=True)
class DatabaseFixture:
    url: str = field(repr=False)

    def migrate(self, revision: str, *, downgrade: bool = False) -> None:
        config = Config(str(ALEMBIC_CONFIG))
        config.attributes['database_url'] = self.url
        if downgrade:
            command.downgrade(config, revision)
        else:
            command.upgrade(config, revision)


@pytest.fixture
def database() -> Iterator[DatabaseFixture]:
    value = os.environ.get('PROCESSING_TEST_DATABASE_URL')
    if not value:
        pytest.fail(
            'Set PROCESSING_TEST_DATABASE_URL to a PostgreSQL _test DB.'
        )
    url = make_url(value)
    if not url.database or not url.database.endswith('_test'):
        pytest.fail('Persistence fixtures require a database ending in _test.')
    schema = f'test_{uuid4().hex}'
    engine = create_database_engine(value)
    try:
        with engine.begin() as connection:
            connection.execute(CreateSchema(schema))
        scoped_url = url.update_query_dict({
            'options': f'-csearch_path={schema}'
        })
        database = DatabaseFixture(
            scoped_url.render_as_string(hide_password=False)
        )
        try:
            database.migrate('head')
            yield database
        finally:
            # This schema was created by this fixture; never clean public or
            # any pre-existing schema/database, even if migrations failed.
            with engine.begin() as connection:
                connection.execute(DropSchema(schema, cascade=True))
    finally:
        engine.dispose()
