import tempfile
import pytest
import os

from pychord.hashing import SHA1Hasher, INTERVAL_SIZE
from pychord.db import open_conn, write_schema


@pytest.fixture
def database_path():
    with tempfile.TemporaryDirectory(prefix="pychord-test") as d:
        yield os.path.join(
            d, "test.db"
        )


@pytest.fixture
def database_conn(database_path):
    conn = open_conn(database_path)
    write_schema(conn)
    yield conn


@pytest.fixture
def interval_size():
    return INTERVAL_SIZE


@pytest.fixture
def hasher(interval_size):
    return SHA1Hasher(size=interval_size)
