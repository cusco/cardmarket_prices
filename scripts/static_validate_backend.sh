#!/usr/bin/env bash

# exit on first non zero return status
set -e

# ensure a failure inside a pipe still fails the script
set -o pipefail

# runs from the repo root. old/ (the retired Django project) is excluded from
# every tool below - it's kept for reference, not maintained, and doesn't need
# to pass any of this.

# run black - make sure everyone uses the same python style
black --skip-string-normalization --line-length 120 --check --exclude "/(old|\.venv)/" .

# run isort for import structure checkup with black profile
isort --atomic --profile black --skip old -c .

# run python static validation
prospector --profile-path=. --profile=.prospector.yml --path=. --ignore-patterns=old

# run bandit - a security linter from OpenStack Security
bandit -r . --exclude ./old,./tests

# run semgrep
semgrep --timeout 60 --config .semgrep_rules.yml --include "*.py" --exclude old --exclude tests .
