import json
from datetime import datetime, timezone

import pytest

import update_formats as uf


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("Mirage", "Mirage"),
        ("Magic: The Gathering—FINAL FANTASY", "FINAL FANTASY"),
        ("Magic: The Gathering® | Teenage Mutant Ninja Turtles", "Teenage Mutant Ninja Turtles"),
        ("Magic: The Gathering® | Marvel Super Heroes", "Marvel Super Heroes"),
        ("Magic: The Gathering® | The Hobbit™", "The Hobbit"),
        # part of the actual name, not crossover branding - must survive stripping
        # on both sides symmetrically (see the module docstring on why).
        ("Magic: The Gathering Foundations", "Foundations"),
    ],
)
def test_normalize_name(raw, expected):
    assert uf._normalize_name(raw) == expected


def test_resolve_to_cardmarket_ids_matches_base_and_extras(monkeypatch):
    monkeypatch.setattr(
        uf,
        "SETS",
        {
            100: {"name": "Test Set"},
            101: {"name": "Test Set: Extras"},
            200: {"name": "Other Set"},
        },
    )
    matched, unmatched = uf.resolve_to_cardmarket_ids(["Test Set"])
    assert matched == {100, 101}
    assert unmatched == []


def test_resolve_to_cardmarket_ids_reports_unmatched(monkeypatch):
    monkeypatch.setattr(uf, "SETS", {100: {"name": "Test Set"}})
    matched, unmatched = uf.resolve_to_cardmarket_ids(["Nonexistent Set"])
    assert matched == set()
    assert unmatched == ["Nonexistent Set"]


def test_resolve_to_cardmarket_ids_skips_known_non_physical_sets(monkeypatch):
    monkeypatch.setattr(uf, "SETS", {})
    monkeypatch.setattr(uf, "NAME_OVERRIDES", {"Digital Only Set": None})
    matched, unmatched = uf.resolve_to_cardmarket_ids(["Digital Only Set"])
    assert matched == set()
    assert unmatched == []  # not a failure - just nothing to resolve


def test_fetch_active_set_names_filters_by_date_window(monkeypatch):
    payload = {
        "sets": [
            {"name": "Not Yet Released", "enterDate": {"exact": "2027-01-01T00:00:00.000"}, "exitDate": None},
            {
                "name": "Already Rotated Out",
                "enterDate": {"exact": "2020-01-01T00:00:00.000"},
                "exitDate": {"exact": "2021-01-01T00:00:00.000"},
            },
            {
                "name": "Currently Active No Exit Date Yet",
                "enterDate": {"exact": "2026-01-01T00:00:00.000"},
                "exitDate": {"exact": None},  # real API shape: exitDate present but "exact" null
            },
            {
                "name": "Currently Active With Future Exit",
                "enterDate": {"exact": "2026-01-01T00:00:00.000"},
                "exitDate": {"exact": "2027-01-01T00:00:00.000"},
            },
        ]
    }

    class FakeResponse:
        def json(self):
            return payload

    monkeypatch.setattr(uf.requests, "get", lambda *a, **k: FakeResponse())

    now = datetime(2026, 6, 1, tzinfo=timezone.utc)
    active = uf.fetch_active_set_names(now=now)

    assert set(active) == {"Currently Active No Exit Date Yet", "Currently Active With Future Exit"}


def test_run_writes_standard_and_unions_pioneer(tmp_path, monkeypatch):
    formats_path = tmp_path / "formats.json"
    formats_path.write_text(json.dumps({"standard": [], "pioneer": [999]}), encoding="utf-8")

    monkeypatch.setattr(uf, "FORMATS_JSON_PATH", formats_path)
    monkeypatch.setattr(uf, "SETS", {100: {"name": "Test Set"}})
    monkeypatch.setattr(uf, "fetch_active_set_names", lambda: ["Test Set"])

    uf.run()

    result = json.loads(formats_path.read_text(encoding="utf-8"))
    assert result["standard"] == [100]
    assert set(result["pioneer"]) == {100, 999}


def test_run_starts_pioneer_empty_when_no_previous_formats_json(tmp_path, monkeypatch):
    formats_path = tmp_path / "formats.json"  # deliberately not created

    monkeypatch.setattr(uf, "FORMATS_JSON_PATH", formats_path)
    monkeypatch.setattr(uf, "SETS", {100: {"name": "Test Set"}})
    monkeypatch.setattr(uf, "fetch_active_set_names", lambda: ["Test Set"])

    uf.run()

    result = json.loads(formats_path.read_text(encoding="utf-8"))
    assert result["pioneer"] == [100]


def test_run_prints_unmatched_names(tmp_path, monkeypatch, capsys):
    formats_path = tmp_path / "formats.json"

    monkeypatch.setattr(uf, "FORMATS_JSON_PATH", formats_path)
    monkeypatch.setattr(uf, "SETS", {})
    monkeypatch.setattr(uf, "fetch_active_set_names", lambda: ["Unknown Set"])

    uf.run()

    captured = capsys.readouterr()
    assert "Unknown Set" in captured.out
