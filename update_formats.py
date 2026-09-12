"""Refresh Standard (and, additively, Pioneer) legal sets from whatsinstandard.com.

whatsinstandard.com's public API (no auth, not Cloudflare-protected) tells us which named sets are Standard-legal
right now. It does NOT know Cardmarket's internal expansion_id numbers - those only exist in sets.json, built from
HTML you paste yourself (see parse_sets.py). So this script can only go as far as:

  1. Fetch the currently-active set names from whatsinstandard.com.
  2. For each, look up its Cardmarket expansion_id (base set + ": Extras") by exact name match against sets.json.
  3. Anything it can't match gets reported, not guessed - either because it's a name that needs an entry in
     NAME_OVERRIDES (see below - e.g. a digital-only companion set with no physical Cardmarket product), or
     because sets.json is stale and needs refreshing (parse_sets.py / merge_sets.py) before this can resolve it.

Standard is rewritten wholesale each run (it's authoritative for "what's active right now"). Pioneer is only ever
unioned with its previous contents - it's a non-rotating format, so once a set is Pioneer-legal it stays that way,
and this script only knows about *currently* Standard-legal sets, not the full ~14-year Pioneer history.

Usage:
    python update_formats.py
"""

import json
import re
from datetime import UTC, datetime

import requests

from constants import FORMATS_JSON_PATH, SETS

WHATS_IN_STANDARD_URL = "https://whatsinstandard.com/api/v6/standard.json"

# whatsinstandard.com name -> None means "known non-physical set, skip silently" (no Cardmarket product exists
# for it, so it's not a resolution failure).
NAME_OVERRIDES: dict[str, str | None] = {
    "Through the Omenpaths": None,  # digital-only companion to Marvel's Spider-Man
}

# whatsinstandard.com uses official branded titles ("Magic: The Gathering® | Teenage Mutant Ninja Turtles") while
# Cardmarket drops that prefix for crossover sets ("Teenage Mutant Ninja Turtles") - but keeps it for sets where
# it's part of the actual name ("Magic: The Gathering Foundations"). Stripping the prefix on BOTH sides before
# comparing keeps both cases matching correctly either way.
_MTG_PREFIX_RE = re.compile(r"^Magic:?\s*The Gathering\s*[®™\-—|:]*\s*", re.IGNORECASE)


def _normalize_name(name: str) -> str:
    name = name.replace("®", "").replace("™", "")
    name = _MTG_PREFIX_RE.sub("", name)
    return re.sub(r"\s+", " ", name).strip()


def fetch_active_set_names(now: datetime | None = None) -> list[str]:
    """Return whatsinstandard.com set names currently inside the Standard window."""

    now = now or datetime.now(UTC)
    data = requests.get(WHATS_IN_STANDARD_URL, timeout=30).json()

    active = []
    for entry in data["sets"]:
        enter = datetime.fromisoformat(entry["enterDate"]["exact"]).replace(tzinfo=UTC)
        exit_info = entry.get("exitDate") or {}
        exit_exact = exit_info.get("exact")
        exit_dt = datetime.fromisoformat(exit_exact).replace(tzinfo=UTC) if exit_exact else None
        if enter <= now and (exit_dt is None or now < exit_dt):
            active.append(entry["name"])
    return active


def resolve_to_cardmarket_ids(set_names: list[str]) -> tuple[set[int], list[str]]:
    """Match whatsinstandard set names to Cardmarket expansion_ids.

    Returns (matched_ids, unmatched_names).
    """

    name_to_id = {_normalize_name(meta["name"]): eid for eid, meta in SETS.items() if meta.get("name")}

    matched: set[int] = set()
    unmatched: list[str] = []
    for name in set_names:
        if name in NAME_OVERRIDES and NAME_OVERRIDES[name] is None:
            continue  # known non-physical set - not a failure, just nothing to add

        cm_name = _normalize_name(NAME_OVERRIDES.get(name, name))
        found = False
        for candidate in (cm_name, f"{cm_name}: Extras"):
            if candidate in name_to_id:
                matched.add(name_to_id[candidate])
                found = True
        if not found:
            unmatched.append(name)

    return matched, unmatched


def run() -> None:
    """Refresh data/formats.json from whatsinstandard.com's current Standard list."""

    previous = json.loads(FORMATS_JSON_PATH.read_text(encoding="utf-8")) if FORMATS_JSON_PATH.exists() else {}
    previous_pioneer = set(previous.get("pioneer", []))

    active_names = fetch_active_set_names()
    standard_ids, unmatched = resolve_to_cardmarket_ids(active_names)
    pioneer_ids = previous_pioneer | standard_ids  # append-only union, never remove

    FORMATS_JSON_PATH.write_text(
        json.dumps({"standard": sorted(standard_ids), "pioneer": sorted(pioneer_ids)}, indent=2),
        encoding="utf-8",
    )

    print(f"Standard: {len(standard_ids)} Cardmarket ids, from {len(active_names)} active whatsinstandard.com sets.")
    print(f"Pioneer: {len(pioneer_ids)} ids ({len(pioneer_ids) - len(previous_pioneer)} newly added).")

    if unmatched:
        print(f"\n{len(unmatched)} set(s) had no Cardmarket match - either add to NAME_OVERRIDES if")
        print("non-physical, or refresh sets.json (parse_sets.py / merge_sets.py) if it's just missing:")
        for name in unmatched:
            print(f"  - {name}")


if __name__ == "__main__":
    run()
