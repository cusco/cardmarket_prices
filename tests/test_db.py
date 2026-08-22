from db import get_connection


def test_get_connection_creates_both_tables(con):
    tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
    assert {"products", "prices"} <= tables


def test_get_connection_is_idempotent(tmp_path):
    db_path = tmp_path / "test.duckdb"

    first = get_connection(db_path)
    first.close()

    second = get_connection(db_path)  # CREATE TABLE IF NOT EXISTS must not error on reopen
    second.close()
