"""Combine two set-metadata sources into the final sets.json used by constants.py.

- data/set_names.json: id -> name for every set Cardmarket currently sells, from parse_sets.py (HTML you copied out
  of your own browser session - the complete, current list).
- data/cardmarket_prices_mtgset_dump.json: a one-time export of the old Django project's MTGSet table, which
  already has code/type/is_foil_only/release_date for most sets (backfilled there in the past via get_set_code()
  and an mtgjson.com matcher, both since abandoned as automated steps but still valid as historical data). See
  old/src/prices/models.py:MTGSet for what "type" values mean - it's Scryfall/MTGJSON's set-type vocabulary
  ("expansion", "promo", "token", "memorabilia", "masters", ...), which is the "is this a real set or
  tokens/promos/junk" flag.

Name always comes from set_names.json when available (it's the more current, complete source);
code/type/is_foil_only/release_date come from the DB dump, when present, else null. Rerun this after re-running
parse_sets.py, or after refreshing data/cardmarket_prices_mtgset_dump.json from the Django DB again.

Usage:
    python merge_sets.py
"""

import json

from constants import PROJECT_DIR

SET_NAMES_JSON_PATH = PROJECT_DIR / "data" / "set_names.json"
MTGSET_DUMP_PATH = PROJECT_DIR / "data" / "cardmarket_prices_mtgset_dump.json"
SETS_JSON_PATH = PROJECT_DIR / "sets.json"


def merge(names: dict, dump: dict) -> dict:
    """Combine {id: name} and {id: {name, code, type, is_foil_only, release_date}}.

    Name prefers `names` (the more current, complete source) when present, else falls back to `dump`'s name.
    Everything else only ever comes from `dump`.
    """

    all_ids = set(names) | set(dump)
    merged = {}
    for expansion_id in all_ids:
        extra = dump.get(expansion_id, {})
        merged[expansion_id] = {
            "name": names.get(expansion_id) or extra.get("name"),
            "code": extra.get("code"),
            "type": extra.get("type"),
            "is_foil_only": extra.get("is_foil_only", False),
            "release_date": extra.get("release_date"),
        }
    return merged


def run() -> None:
    """Merge the two sources on disk and write the result to sets.json."""

    names = json.loads(SET_NAMES_JSON_PATH.read_text(encoding="utf-8"))
    dump = json.loads(MTGSET_DUMP_PATH.read_text(encoding="utf-8")) if MTGSET_DUMP_PATH.exists() else {}

    merged = merge(names, dump)
    SETS_JSON_PATH.write_text(json.dumps(merged, indent=2, sort_keys=True), encoding="utf-8")

    with_code = sum(1 for v in merged.values() if v["code"])
    with_type = sum(1 for v in merged.values() if v["type"])
    print(f"Merged {len(merged)} sets -> {SETS_JSON_PATH} ({with_code} with code, {with_type} with type)")


if __name__ == "__main__":
    run()
