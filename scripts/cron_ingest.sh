#!/usr/bin/env bash

# Daily cron entrypoint - ingests the latest Cardmarket price snapshot, and
# pushes to Google Sheets if (and only if) that ingest actually found something
# new (see daily.py). Self-contained: cron runs scripts with none of your
# shell profile sourced, so this calls the virtualenv's python binary directly
# rather than relying on `source .../bin/activate` (which needs an interactive
# shell). Both paths below are explicit rather than self-located from the
# script's own path - deliberately, since that kind of introspection turned
# out to be unreliable in testing.
#
# Paths below are already filled in for ovh.tretas.eu (repo at
# ~/git/cardmarket_prices, virtualenvwrapper env "cardmarket_prices"). Adjust
# if this runs somewhere else. No output redirection needed in the crontab
# line - this script logs to local/cron_ingest.log itself.
#
# Timing: real data (12 consecutive days of file mtimes on this server, see
# git history) shows Cardmarket's daily snapshot consistently lands 01:43-
# 01:49 server time. Scheduled with a >20-minute buffer past that.
#   crontab -e:
#     10 2 * * * /home/cusco/git/cardmarket_prices/scripts/cron_ingest.sh

set -e

REPO_DIR="/home/cusco/git/cardmarket_prices"
VENV_PYTHON="/home/cusco/.virtualenvs/cardmarket_prices/bin/python"

LOG_DIR="$REPO_DIR/local"
mkdir -p "$LOG_DIR"

cd "$REPO_DIR"
exec >> "$LOG_DIR/cron_ingest.log" 2>&1

echo "=== $(date -Iseconds) ==="
"$VENV_PYTHON" daily.py
