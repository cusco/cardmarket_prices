"""DuckDB connection + schema for price_pulse."""

import duckdb

from constants import DB_PATH

SCHEMA = """
CREATE TABLE IF NOT EXISTS products (
    cm_id BIGINT PRIMARY KEY,
    name VARCHAR,
    expansion_id INTEGER,
    metacard_id BIGINT,
    category_id INTEGER
);

CREATE TABLE IF NOT EXISTS prices (
    cm_id BIGINT,
    catalog_date DATE,
    avg DOUBLE,
    low DOUBLE,
    trend DOUBLE,
    avg1 DOUBLE,
    avg7 DOUBLE,
    avg30 DOUBLE,
    avg_foil DOUBLE,
    low_foil DOUBLE,
    trend_foil DOUBLE,
    avg1_foil DOUBLE,
    avg7_foil DOUBLE,
    avg30_foil DOUBLE,
    PRIMARY KEY (cm_id, catalog_date)
);
"""


def get_connection(db_path=DB_PATH) -> duckdb.DuckDBPyConnection:
    """Open (creating if needed) the price_pulse DuckDB database with its schema."""

    con = duckdb.connect(str(db_path))
    con.execute(SCHEMA)
    return con
