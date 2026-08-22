import json

import pytest

import parse_sets
from parse_sets import parse_sets_html

SAMPLE_HTML = """
<select name="idExpansion">
<option value="0">All</option>
<option value="16">Mirage</option>
<option value="94">Champs &amp; States Promos</option>
<option value="1833" selected="selected">Rk post Products</option>
</select>
"""


@pytest.fixture
def parsed(tmp_path):
    html_file = tmp_path / "sets.html"
    html_file.write_text(SAMPLE_HTML, encoding="utf-8")
    return parse_sets_html(html_file)


def test_skips_the_all_placeholder_option(parsed):
    assert 0 not in parsed


def test_decodes_html_entities(parsed):
    assert parsed[94] == "Champs & States Promos"


def test_ignores_extra_attributes_like_selected(parsed):
    assert parsed[1833] == "Rk post Products"


def test_basic_entry(parsed):
    assert parsed[16] == "Mirage"


def test_parsed_count_matches_non_all_options(parsed):
    assert len(parsed) == 3


def test_run_writes_set_names_json(tmp_path, monkeypatch):
    html_path = tmp_path / "cardmarket_sets.html"
    out_path = tmp_path / "set_names.json"
    html_path.write_text(SAMPLE_HTML, encoding="utf-8")

    monkeypatch.setattr(parse_sets, "RAW_HTML_PATH", html_path)
    monkeypatch.setattr(parse_sets, "SET_NAMES_JSON_PATH", out_path)

    parse_sets.run()

    result = json.loads(out_path.read_text(encoding="utf-8"))
    assert result["16"] == "Mirage"
