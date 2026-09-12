import daily


def test_run_exports_when_new_rows_arrived(monkeypatch):
    monkeypatch.setattr(daily, "run_ingest", lambda: 5)

    calls = []
    monkeypatch.setattr(daily, "export_spikes_to_gdrive", lambda: calls.append("spikes") or "ok")
    monkeypatch.setattr(daily, "export_premodern_bulk_to_gdrive", lambda: calls.append("premodern_bulk") or "ok")

    daily.run()

    assert calls == ["spikes", "premodern_bulk"]


def test_run_skips_export_when_nothing_new(monkeypatch):
    monkeypatch.setattr(daily, "run_ingest", lambda: 0)

    calls = []
    monkeypatch.setattr(daily, "export_spikes_to_gdrive", lambda: calls.append("spikes") or "ok")
    monkeypatch.setattr(daily, "export_premodern_bulk_to_gdrive", lambda: calls.append("premodern_bulk") or "ok")

    daily.run()

    assert calls == []
