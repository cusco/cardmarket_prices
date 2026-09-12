#!/usr/bin/env bash

# Daily cron entrypoint - ingests the latest Cardmarket price snapshot, and pushes to Google Sheets if (and only
# if) that ingest actually found something new (see daily.py). Self-contained: cron runs scripts with none of your
# shell profile sourced, so this calls the virtualenv's python binary directly rather than relying on
# `source .../bin/activate` (which needs an interactive shell). Both paths below are explicit rather than
# self-located from the script's own path - deliberately, since that kind of introspection turned out to be
# unreliable in testing.
#
# Paths below are already filled in for ovh.tretas.eu (repo at ~/git/cardmarket_prices, virtualenvwrapper env
# "cardmarket_prices"). Adjust if this runs somewhere else. No output redirection needed in the crontab line -
# this script logs to LOG_FILE itself. daily.py's own logging is verbose (one line per already-ingested file,
# useful when run by hand) - that full output is captured but not kept, only a single summary line per run gets
# appended to LOG_FILE.
#
# LOG_FILE lives under local/ (cusco-owned, gitignored), not /var/log/custom/ like this server's other cron jobs -
# that directory's shared logrotate policy (/etc/logrotate.d/custom) recreates rotated files as root:adm 640 on
# every rotation, which silently broke this script's write access for weeks (daily.py itself kept running fine;
# only the log line after it failed, under `set -e`, which then reported the whole run as failed by email every
# night). A single one-line-per-day summary never needs rotation anyway, so this sidesteps the problem entirely
# rather than fighting root-owned config shared with unrelated services.
#
# Timing: real data (12 consecutive days of file mtimes on this server, see git history) shows Cardmarket's daily
# snapshot consistently lands 01:43-01:49 server time. Scheduled with a >20-minute buffer past that.
#   crontab -e:
#     10 2 * * * /home/cusco/git/cardmarket_prices/scripts/cron_ingest.sh

set -e

REPO_DIR="/home/cusco/git/cardmarket_prices"
VENV_PYTHON="/home/cusco/.virtualenvs/cardmarket_prices/bin/python"
LOG_FILE="$REPO_DIR/local/cron_ingest.log"

cd "$REPO_DIR"

timestamp="$(date -Iseconds)"
if output="$("$VENV_PYTHON" daily.py 2>&1)"; then
    summary="$(echo "$output" | grep -o 'Done: [0-9]* new price rows, [0-9]* products in catalog\.' | tail -1)"
    if echo "$output" | grep -q 'exporting to Google Sheets'; then
        export_status="export=yes"
    else
        export_status="export=no"
    fi
    echo "$timestamp | OK ${summary:-(no summary line found)} $export_status" >> "$LOG_FILE"
else
    echo "$timestamp | FAILED: $(echo "$output" | tail -1)" >> "$LOG_FILE"
    exit 1
fi
