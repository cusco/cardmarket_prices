import pytest

from db import get_connection


@pytest.fixture
def con():
    """Fresh in-memory DuckDB connection with schema, closed after the test."""

    connection = get_connection(":memory:")
    yield connection
    connection.close()
