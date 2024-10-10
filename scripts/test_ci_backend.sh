#!/usr/bin/env bash

export DJANGO_SETTINGS_MODULE=cm_prices.settings
# py.test -n 4 --disable-socket --nomigrations --reuse-db -W error::RuntimeWarning --cov=src --cov-report=html tests/

cd src/
coverage run manage.py test
coverage report
coverage html
cd $OLDPWD
