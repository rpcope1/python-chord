from sqlite3 import dbapi2 as sqlite
from contextlib import contextmanager
import json
from typing import Any, Dict


SCHEMA = """
CREATE TABLE IF NOT EXISTS kv_store(
   key TEXT PRIMARY KEY NOT NULL,
   value TEXT NOT NULL
);
"""


def open_conn(*args, **kwargs) -> sqlite.Connection:
    conn = sqlite.connect(*args, **kwargs)
    conn.row_factory = sqlite.Row
    return conn


@contextmanager
def transaction_wrapper(connection: sqlite.Connection) -> sqlite.Connection:
    try:
        connection.execute("BEGIN DEFERRED TRANSACTION")
        yield connection
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()


@contextmanager
def cursor_manager(connection):
    cursor = connection.cursor()
    yield cursor
    cursor.close()


def write_schema(connnection: sqlite.Connection):
    connnection.executescript(SCHEMA)


def get_value_by_key(conn: sqlite.Connection, key, default=None) -> Any:
    with cursor_manager(conn) as c:
        c.execute(
            "SELECT value FROM kv_store WHERE key = ?",
            (key,)
        )
        row = c.fetchone()
        if row:
            return json.loads(row["value"])
        else:
            return default


def get_all_kv_pairs(conn: sqlite.Connection) -> Dict[str, Any]:
    with cursor_manager(conn) as c:
        c.execute(
            "SELECT key, value FROM kv_store",
        )
        return {
            row["key"]: json.loads(row["value"]) for row in c.fetchall()
        }


def does_key_exist(conn: sqlite.Connection, key) -> bool:
    with cursor_manager(conn) as c:
        c.execute(
            "SELECT 1 FROM kv_store WHERE key = ?",
            (key,)
        )
        return bool(c.fetchone())


def set_key_value_pair(conn: sqlite.Connection, key: str, value: Any):
    with cursor_manager(conn) as c:
        c.execute(
            "INSERT OR REPLACE INTO kv_store(key, value) VALUES (?, ?)",
            (key, json.dumps(value))
        )


def remove_key(conn: sqlite.Connection, key: str):
    with cursor_manager(conn) as c:
        c.execute(
            "DELETE FROM kv_store WHERE key = ?",
            (key,)
        )
