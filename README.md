![](https://github.com/hasadna/open-bus-gtfs-etl/actions/workflows/CI.yml/badge.svg?branch=main) [![codecov](https://codecov.io/gh/hasadna/open-bus-gtfs-etl/branch/main/graph/badge.svg?token=JJDM2TRBA8)](https://codecov.io/gh/hasadna/open-bus-gtfs-etl)

# open-bus-gtfs-etl
GTFS ETL for Stride.

See [our contributing docs](https://github.com/hasadna/open-bus-pipelines/blob/main/CONTRIBUTING.md) if you want to suggest changes to this repository.

## Development using the Docker Compose environment

This is the easiest option to start development, follow these instructions: https://github.com/hasadna/open-bus-pipelines/blob/main/README.md#gtfs-etl

For local development, see the additional functionality section: `Develop gtfs-etl from a local clone`

## Development using local Python interpreter

It's much easier to use the Docker Compose environment, but the following can be
referred to for more details regarding the internal processes and for development
using your local Python interpreter. 

### Install

[install levelDB](https://github.com/akariv/kvfile#installing-leveldb)

Create virtualenv (Python 3.8)

```
python3.8 -m venv venv
```

Upgrade pip

```
venv/bin/pip install --upgrade pip
```

You should have a clone of the following repositories in sibling directories:

* `../open-bus-stride-db`: https://github.com/hasadna/open-bus-stride-db

Install dev requirements (this installs above repositories as well as this repository as editable for development):

```
pip install -r requirements-dev.txt
```

Create a `.env` file and set the following in the file:

The sql alchemy url should be as follows (it's only used locally):

```
export SQLALCHEMY_URL=postgresql://postgres:123456@localhost
```

### Use

Use the Docker-compose environment in open-bus-pipelines to start a DB:

* Follow [these instructions to start the DB](https://github.com/hasadna/open-bus-pipelines/blob/main/README.md#stride-db)

Activate the virtualenv and source the .env file

```
. venv/bin/activate
source .env
```

See the help message for available tasks:

```
open-bus-gtfs-etl --help
```

See [open_bus_gtfs_etl/dags.yaml](open_bus_gtfs_etl/dags.yaml) for the steps which run on Airflow. 

### Supported Operations and Configurations

#### Environment variables

See [open_bus_gtfs_etl/config.py](open_bus_gtfs_etl/config.py) for the relevant environment variables.

DB connection is managed by [open-bus-stride-db](https://github.com/hasadna/open-bus-stride-db),
see that project's README for details.

#### Supported Operations 

See the CLI Help message for details.

### Tests and code before refactoring

Following commit has tests and gtfs stats code which was deleted, it might be useful in the future
in case we will want to use these tests or the code for reference:
https://github.com/hasadna/open-bus-gtfs-etl/tree/93d313d5a039f5297dd95f96c3e5eb2f00b4cc72
