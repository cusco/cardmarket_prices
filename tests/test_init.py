import init

SAMPLE_LISTING_HTML = """
<table>
<tr><td><a href="../">../</a></td></tr>
<tr><td><a href="2025-01-01_abc_price_guide_1.json.gz">2025-01-01_abc_price_guide_1.json.gz</a></td></tr>
<tr><td><a href="subdir/">subdir/</a></td></tr>
</table>
"""


def test_is_first_run_true_on_empty_db(con):
    assert init.is_first_run(con) is True


def test_is_first_run_false_after_insert(con):
    con.execute("INSERT INTO products VALUES (1, 'Test Card', 16, 1, 1)")
    con.execute("INSERT INTO prices (cm_id, catalog_date, trend) VALUES (1, '2026-01-01', 1.0)")
    assert init.is_first_run(con) is False


def test_fetch_remote_catalog_listing_extracts_only_gz_links(monkeypatch):
    class FakeResponse:
        text = SAMPLE_LISTING_HTML

        def raise_for_status(self):
            pass

    monkeypatch.setattr(init.requests, "get", lambda *a, **k: FakeResponse())

    urls = init.fetch_remote_catalog_listing("https://example.com/catalogs/")

    assert urls == ["https://example.com/catalogs/2025-01-01_abc_price_guide_1.json.gz"]


def test_download_catalog_skips_existing_file(tmp_path, monkeypatch):
    existing = tmp_path / "already_here.json.gz"
    existing.write_bytes(b"data")

    def fail_if_called(*args, **kwargs):
        raise AssertionError("should not fetch a file that's already downloaded")

    monkeypatch.setattr(init.requests, "get", fail_if_called)

    downloaded = init.download_catalog("https://example.com/already_here.json.gz", tmp_path)

    assert downloaded is False


def test_download_catalog_writes_new_file(tmp_path, monkeypatch):
    class FakeResponse:
        content = b"fake gzip bytes"

        def raise_for_status(self):
            pass

    monkeypatch.setattr(init.requests, "get", lambda *a, **k: FakeResponse())

    downloaded = init.download_catalog("https://example.com/new_file.json.gz", tmp_path)

    assert downloaded is True
    assert (tmp_path / "new_file.json.gz").read_bytes() == b"fake gzip bytes"


def test_bootstrap_from_remote_skips_when_unconfigured(monkeypatch):
    monkeypatch.setattr(init, "REMOTE_CATALOGS_URL", "")
    assert init.bootstrap_from_remote() == 0


def test_bootstrap_from_remote_downloads_whatever_the_listing_has(tmp_path, monkeypatch):
    monkeypatch.setattr(init, "REMOTE_CATALOGS_URL", "https://example.com/catalogs/")
    monkeypatch.setattr(
        init,
        "fetch_remote_catalog_listing",
        lambda url: ["https://example.com/catalogs/a.json.gz", "https://example.com/catalogs/b.json.gz"],
    )

    downloaded_urls = []

    def fake_download(url, catalogs_dir):
        downloaded_urls.append(url)
        return True

    monkeypatch.setattr(init, "download_catalog", fake_download)

    count = init.bootstrap_from_remote(tmp_path)

    assert count == 2
    assert downloaded_urls == ["https://example.com/catalogs/a.json.gz", "https://example.com/catalogs/b.json.gz"]


def test_bootstrap_from_remote_only_counts_actual_downloads(tmp_path, monkeypatch):
    monkeypatch.setattr(init, "REMOTE_CATALOGS_URL", "https://example.com/catalogs/")
    monkeypatch.setattr(
        init,
        "fetch_remote_catalog_listing",
        lambda url: ["https://example.com/catalogs/already_here.json.gz"],
    )
    monkeypatch.setattr(init, "download_catalog", lambda url, catalogs_dir: False)  # already existed

    count = init.bootstrap_from_remote(tmp_path)

    assert count == 0


def test_run_if_first_time_skips_bootstrap_on_populated_db(con, monkeypatch):
    con.execute("INSERT INTO products VALUES (1, 'Test Card', 16, 1, 1)")
    con.execute("INSERT INTO prices (cm_id, catalog_date, trend) VALUES (1, '2026-01-01', 1.0)")

    calls = []
    monkeypatch.setattr(init, "bootstrap_from_remote", lambda *a, **k: calls.append(1))

    ran = init.run_if_first_time(con)

    assert ran is False
    assert calls == []


def test_run_if_first_time_runs_bootstrap_on_empty_db(con, monkeypatch):
    calls = []

    def fake_bootstrap(*args, **kwargs):
        calls.append(1)
        return 0

    monkeypatch.setattr(init, "bootstrap_from_remote", fake_bootstrap)

    ran = init.run_if_first_time(con)

    assert ran is True
    assert calls == [1]
