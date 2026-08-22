import json

import merge_sets
from merge_sets import merge


def test_name_prefers_names_source_over_dump():
    merged = merge(
        names={1: "Current Name"},
        dump={1: {"name": "Stale Name", "code": "COD"}},
    )
    assert merged[1]["name"] == "Current Name"
    assert merged[1]["code"] == "COD"


def test_falls_back_to_dump_name_when_missing_from_names():
    merged = merge(names={}, dump={1: {"name": "Only In Dump"}})
    assert merged[1]["name"] == "Only In Dump"


def test_set_only_in_names_has_null_metadata():
    merged = merge(names={1: "New Set"}, dump={})
    assert merged[1] == {
        "name": "New Set",
        "code": None,
        "type": None,
        "is_foil_only": False,
        "release_date": None,
    }


def test_is_foil_only_defaults_to_false():
    merged = merge(names={1: "Set"}, dump={1: {}})
    assert merged[1]["is_foil_only"] is False


def test_union_of_ids_from_both_sources():
    merged = merge(names={1: "A"}, dump={2: {"name": "B"}})
    assert set(merged) == {1, 2}


def test_run_reads_both_sources_and_writes_sets_json(tmp_path, monkeypatch):
    names_path = tmp_path / "set_names.json"
    dump_path = tmp_path / "dump.json"
    sets_path = tmp_path / "sets.json"

    names_path.write_text(json.dumps({"1": "Alpha"}), encoding="utf-8")
    dump_path.write_text(json.dumps({"1": {"name": "Alpha", "code": "LEA"}}), encoding="utf-8")

    monkeypatch.setattr(merge_sets, "SET_NAMES_JSON_PATH", names_path)
    monkeypatch.setattr(merge_sets, "MTGSET_DUMP_PATH", dump_path)
    monkeypatch.setattr(merge_sets, "SETS_JSON_PATH", sets_path)

    merge_sets.run()

    result = json.loads(sets_path.read_text(encoding="utf-8"))
    assert result["1"]["name"] == "Alpha"
    assert result["1"]["code"] == "LEA"


def test_run_treats_a_missing_dump_file_as_empty(tmp_path, monkeypatch):
    names_path = tmp_path / "set_names.json"
    dump_path = tmp_path / "dump.json"  # deliberately not created
    sets_path = tmp_path / "sets.json"

    names_path.write_text(json.dumps({"1": "Alpha"}), encoding="utf-8")

    monkeypatch.setattr(merge_sets, "SET_NAMES_JSON_PATH", names_path)
    monkeypatch.setattr(merge_sets, "MTGSET_DUMP_PATH", dump_path)
    monkeypatch.setattr(merge_sets, "SETS_JSON_PATH", sets_path)

    merge_sets.run()

    result = json.loads(sets_path.read_text(encoding="utf-8"))
    assert result["1"]["name"] == "Alpha"
    assert result["1"]["code"] is None
