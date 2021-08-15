#!/usr/bin/env bash

flake8 open_bus_gtfs_etl --count --select=E9,F63,F7,F82 --show-source --statistics &&\
flake8 open_bus_gtfs_etl --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics
