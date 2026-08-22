import gzip
import json
from datetime import date

import ingest as ingest_module
from ingest import (
    _date_from_filename,
    fetch_latest_catalog,
    find_local_catalog_files,
    ingest_all,
    ingest_file,
    refresh_products,
)


def _write_price_guide_fixture(path, created_at="2026-01-01T00:00:00+0000", entries=None):
    if entries is None:
        entries = [{"idProduct": 1, "avg": 1.5, "low": 1.0, "trend": 1.2, "avg1": 1.1, "avg7": 1.3, "avg30": 1.4}]
    payload = {"version": 1, "createdAt": created_at, "priceGuides": entries}
    with gzip.open(path, "wt", encoding="utf-8") as f:
        json.dump(payload, f)


def test_ingest_file_inserts_rows(con, tmp_path):
    fixture = tmp_path / "sample_price_guide_1.json.gz"
    _write_price_guide_fixture(fixture)

    rows = ingest_file(con, fixture)

    assert rows == 1
    result = con.execute("SELECT cm_id, catalog_date, trend FROM prices").fetchall()
    assert result == [(1, date(2026, 1, 1), 1.2)]


def test_ingest_file_skips_already_ingested_date(con, tmp_path):
    fixture = tmp_path / "sample_price_guide_1.json.gz"
    _write_price_guide_fixture(fixture)

    ingest_file(con, fixture)
    second_pass_rows = ingest_file(con, fixture)

    assert second_pass_rows == 0
    (count,) = con.execute("SELECT count(*) FROM prices").fetchone()
    assert count == 1


def test_date_from_filename_parses_the_leading_date(tmp_path):
    path = tmp_path / "2026-08-22_abc123_price_guide_1.json.gz"
    assert _date_from_filename(path) == date(2026, 8, 22)


def test_date_from_filename_returns_none_without_a_date_prefix(tmp_path):
    path = tmp_path / "sample_price_guide_1.json.gz"
    assert _date_from_filename(path) is None


def test_ingest_file_skips_already_ingested_date_via_filename_without_opening_the_file(con, tmp_path):
    """The whole point of the filename shortcut: an already-known date never gets parsed.

    Proven here with a file that would raise if it were actually opened - if this
    test passes, the shortcut is what skipped it, not a lucky coincidence.
    """

    con.execute("INSERT INTO prices (cm_id, catalog_date, trend) VALUES (1, '2026-08-22', 1.0)")

    corrupt_file = tmp_path / "2026-08-22_abc123_price_guide_1.json.gz"
    corrupt_file.write_bytes(b"this is not valid gzip data")

    rows = ingest_file(con, corrupt_file)

    assert rows == 0


def test_ingest_file_falls_back_to_real_parse_when_filename_date_is_wrong(con, tmp_path):
    """A misleading filename can't corrupt data - it just loses the fast path."""

    # nothing ingested yet for either date, so the filename guess (Jan 1) can't
    # short-circuit anything - the file must be opened, and its real createdAt
    # (Jan 2) is what actually gets stored.
    fixture = tmp_path / "2026-01-01_price_guide_1.json.gz"
    _write_price_guide_fixture(fixture, created_at="2026-01-02T00:00:00+0000")

    rows = ingest_file(con, fixture)

    assert rows == 1
    result = con.execute("SELECT catalog_date FROM prices").fetchall()
    assert result == [(date(2026, 1, 2),)]


def test_ingest_file_skips_rows_with_null_id_product(con, tmp_path):
    fixture = tmp_path / "sample_price_guide_1.json.gz"
    _write_price_guide_fixture(
        fixture,
        entries=[
            {"idProduct": 1, "trend": 1.0},
            {"idProduct": None, "trend": 2.0},
        ],
    )

    rows = ingest_file(con, fixture)

    assert rows == 1


def test_find_local_catalog_files_returns_sorted_gz_files(tmp_path):
    (tmp_path / "b_price_guide_1.json.gz").write_bytes(b"")
    (tmp_path / "a_price_guide_1.json.gz").write_bytes(b"")
    (tmp_path / "not_a_price_guide.txt").write_bytes(b"")

    files = find_local_catalog_files(tmp_path)

    assert [f.name for f in files] == ["a_price_guide_1.json.gz", "b_price_guide_1.json.gz"]


def test_ingest_all_sums_rows_across_files(con, tmp_path):
    _write_price_guide_fixture(tmp_path / "a_price_guide_1.json.gz", created_at="2026-01-01T00:00:00+0000")
    _write_price_guide_fixture(tmp_path / "b_price_guide_1.json.gz", created_at="2026-01-02T00:00:00+0000")

    total = ingest_all(con, tmp_path)

    assert total == 2


def test_fetch_latest_catalog_writes_new_gz_file(tmp_path, monkeypatch):
    class FakeResponse:
        text = json.dumps({"version": 1, "createdAt": "2026-01-01T00:00:00+0000", "priceGuides": []})

        def raise_for_status(self):
            pass

    monkeypatch.setattr(ingest_module.requests, "get", lambda *a, **k: FakeResponse())

    path = fetch_latest_catalog(tmp_path)

    assert path is not None
    assert path.exists()
    with gzip.open(path, "rt", encoding="utf-8") as f:
        assert json.load(f)["createdAt"] == "2026-01-01T00:00:00+0000"


def test_fetch_latest_catalog_skips_duplicate_by_content_hash(tmp_path, monkeypatch):
    content = json.dumps({"version": 1, "createdAt": "2026-01-01T00:00:00+0000", "priceGuides": []})

    class FakeResponse:
        text = content

        def raise_for_status(self):
            pass

    monkeypatch.setattr(ingest_module.requests, "get", lambda *a, **k: FakeResponse())

    first = fetch_latest_catalog(tmp_path)
    second = fetch_latest_catalog(tmp_path)

    assert first is not None
    assert second is None  # same content hash - already archived


def test_refresh_products_replaces_table(con, monkeypatch):
    payload = {
        "createdAt": "2026-01-01T00:00:00+0000",
        "products": [
            {"idProduct": 1, "name": "Test Card", "idExpansion": 16, "idCategory": 1, "idMetacard": 100},
        ],
    }

    class FakeResponse:
        text = json.dumps(payload)

        def raise_for_status(self):
            pass

    monkeypatch.setattr(ingest_module.requests, "get", lambda *a, **k: FakeResponse())

    count = refresh_products(con)

    assert count == 1
    result = con.execute("SELECT cm_id, name, expansion_id FROM products").fetchall()
    assert result == [(1, "Test Card", 16)]


def test_run_orchestrates_the_pipeline_in_order_and_closes_connection(monkeypatch):
    class FakeConnection:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    fake_con = FakeConnection()
    calls = []

    monkeypatch.setattr(ingest_module, "get_connection", lambda: fake_con)
    monkeypatch.setattr(ingest_module, "run_if_first_time", lambda con: calls.append("first_time"))
    monkeypatch.setattr(ingest_module, "fetch_latest_catalog", lambda: calls.append("fetch"))
    monkeypatch.setattr(ingest_module, "ingest_all", lambda con: calls.append("ingest_all") or 5)
    monkeypatch.setattr(ingest_module, "refresh_products", lambda con: calls.append("refresh") or 3)

    result = ingest_module.run()

    assert calls == ["first_time", "fetch", "ingest_all", "refresh"]
    assert fake_con.closed is True
    assert result == 5  # the row count ingest_all() reported, not refresh_products()'s
