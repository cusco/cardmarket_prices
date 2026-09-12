#!/usr/bin/env bash

# exit on first non zero return status
set -e

# ensure a failure inside a pipe still fails the script
set -o pipefail

# runs from the repo root. old/ (the retired Django project) is excluded via pyproject.toml's
# [tool.ruff] extend-exclude - it's kept for reference, not maintained, and doesn't need to pass any of this.

# run ruff format - make sure everyone uses the same python style
ruff format --check .

# run ruff check - linting, import sorting, complexity, and bandit-style security checks (see pyproject.toml).
# In CI, --output-format=github turns findings into inline annotations on the PR diff instead of plain stdout.
if [ "$GITHUB_ACTIONS" = "true" ]; then
    ruff check --output-format=github .
else
    ruff check .
fi
