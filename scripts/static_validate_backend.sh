#!/usr/bin/env bash

export DJANGO_SETTINGS_MODULE=cm_prices.settings

# run black - make sure everyone uses the same python style
black --skip-string-normalization --line-length 120 --check src

# run isort for import structure checkup with black profile
isort --atomic --profile black -c src
# isort --atomic --profile black -c tests

# change to src directory to run all the necessary scripts on the correct path
cd src || exit 1

# run django migrations check to ensure that there are no migrations left to create
python manage.py makemigrations --check --dry-run

# run python static validation
prospector  --profile-path=. --profile=../.prospector.yml --path=. --ignore-patterns=static

# run bandit - A security linter from OpenStack Security
bandit -r .


## run mypy
#cd django || exit
#mypy .
#cd ..


# run semgrep
semgrep --timeout 60 --config ../.semgrep_rules.yml .
