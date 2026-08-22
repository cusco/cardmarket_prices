"""Parse Cardmarket's `idExpansion` <select> dropdown HTML into id -> set name JSON.

The dropdown isn't reachable by script anymore (Cloudflare challenge), so this
expects HTML copied by hand from your own logged-in browser session - e.g. from
"View Page Source" or copying the <select id="idExpansion..."> element on any
Cardmarket "Magic Singles" search page. Save it to data/cardmarket_sets.html and
rerun this whenever you want to refresh the set list (new sets, renamed sets).

Usage:
    python parse_sets.py
"""

import json
from html.parser import HTMLParser

from constants import PROJECT_DIR

RAW_HTML_PATH = PROJECT_DIR / "data" / "cardmarket_sets.html"
SET_NAMES_JSON_PATH = PROJECT_DIR / "data" / "set_names.json"


class _OptionParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.sets: dict[int, str] = {}
        self._current_id: int | None = None
        self._buffer = ""

    def handle_starttag(self, tag, attrs):
        if tag != "option":
            return
        attrs = dict(attrs)
        value = attrs.get("value")
        if value is None or value == "0":  # skip the "All" placeholder option
            return
        self._current_id = int(value)
        self._buffer = ""

    def handle_data(self, data):
        if self._current_id is not None:
            self._buffer += data

    def handle_endtag(self, tag):
        if tag == "option" and self._current_id is not None:
            self.sets[self._current_id] = self._buffer.strip()
            self._current_id = None


def parse_sets_html(html_path=RAW_HTML_PATH) -> dict[int, str]:
    """Parse the saved dropdown HTML into {expansion_id: set_name}."""

    parser = _OptionParser()
    parser.feed(html_path.read_text(encoding="utf-8"))
    return parser.sets


def run() -> None:
    """Parse the saved HTML on disk and write the result to data/set_names.json."""

    sets = parse_sets_html()
    SET_NAMES_JSON_PATH.write_text(json.dumps(sets, indent=2, sort_keys=True), encoding="utf-8")
    print(f"Parsed {len(sets)} sets -> {SET_NAMES_JSON_PATH}")
    print("Run merge_sets.py next to fold in code/type from the old cardmarket_prices DB.")


if __name__ == "__main__":
    run()
