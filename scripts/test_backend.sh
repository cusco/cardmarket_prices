#!/usr/bin/env bash

# exit on first non zero return status
set -e

# ensure a failure inside a pipe still fails the script
set -o pipefail

# runs from the repo root - pytest.ini + .coveragerc live there and point at tests/. Produces htmlcov/ (coverage
# HTML), coverage.xml, and report_pytest.xml (junit) - all three get uploaded as CI artifacts, see
# .github/workflows/ci.yml.
pytest
