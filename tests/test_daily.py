import daily


def test_run_exports_when_new_rows_arrived(monkeypatch):
    monkeypatch.setattr(daily, "run_ingest", lambda: 5)

    calls = []
    monkeypatch.setattr(daily, "export_spikes_to_gdrive", lambda: calls.append("exported") or "ok")

    daily.run()

    assert calls == ["exported"]


def test_run_skips_export_when_nothing_new(monkeypatch):
    monkeypatch.setattr(daily, "run_ingest", lambda: 0)

    calls = []
    monkeypatch.setattr(daily, "export_spikes_to_gdrive", lambda: calls.append("exported") or "ok")

    daily.run()

    assert calls == []
