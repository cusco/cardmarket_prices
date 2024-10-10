#!/usr/bin/env bash

export DJANGO_SETTINGS_MODULE=cm_prices.settings

# exit on first non zero return status
set -e

# ensure tee passes the error of the test tool
# https://stackoverflow.com/questions/6871859/piping-command-output-to-tee-but-also-save-exit-code-of-command
set -o pipefail

# run black - make sure everyone uses the same python style
black --skip-string-normalization --line-length 120 --check src

# run isort for import structure checkup with black profile
isort --atomic --profile black -c src

# run semgrep
semgrep --timeout 60 --strict --error --config .semgrep_rules.yml --junit-xml -o report_semgrep.xml src/

# change to src directory to run all the necessary scripts on the correct path
cd src || exit 1

# run django migrations check to ensure that there are no migrations left to create
python manage.py makemigrations --check --dry-run

# run mypy
# mypy --junit-xml ../report_mypy.xml .

# run bandit - A security linter from OpenStack Security
bandit -r .

# run python static validation
prospector  --profile-path=. --profile=../.prospector.yml --path=. --ignore-patterns=static --output-format=xunit |& tee ../report_prospector.xml