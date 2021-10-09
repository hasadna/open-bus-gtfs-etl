![](https://github.com/hasadna/open-bus-gtfs-etl/actions/workflows/CI.yml/badge.svg?branch=main) [![codecov](https://codecov.io/gh/hasadna/open-bus-gtfs-etl/branch/main/graph/badge.svg?token=JJDM2TRBA8)](https://codecov.io/gh/hasadna/open-bus-gtfs-etl)

# open-bus-gtfs-etl
GTFS ETL for Stride.

## Development using the Docker Compose environment

This is the easiest option to start development, follow these instructions: https://github.com/hasadna/open-bus-pipelines/blob/main/README.md#gtfs-etl

For local development, see the additional functionality section: `Develop gtfs-etl from a local clone`

## Development using local Python interpreter

It's much easier to use the Docker Compose environment, but the following can be
referred to for more details regarding the internal processes and for development
using your local Python interpreter. 

### Install

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

### Lint / Tests

Install tests requirements

```
pip install -r tests/requirements.txt
```

Lint

```
bin/lint.sh
```

Test

```
pytest
```

### Supported Operations and Configurations
#### Environment variables
- **gtfs_etl_root_archives_folder** - the local folder that will contain a sub folder "gtfs_archive" 
  for downloaded gtfs files from MOT and "stat_archive" for analyzed files that could be uploaded to db.
  the default path if nothing will be ./.data
  ```commandline
  export gtfs_etl_root_archives_folder=./mydata
  ```
  
### Supported Operations 
- **Download the daily gtfs data** and store in a directory structure (e.g. gtfs_data/YEAR/MONTH/DAY)
  for example:
  ```commandline
  python cli.py download-gtfs-into-archive
  ```

- **Analyzing the daily gtfs data** and store in a directory structure (e.g. stat_data/YEAR/MONTH/DAY)
  this operation has optional parameter **--date-to-analyze** that get date (format: %Y-%m-%d). 
  This param by default uses machine current date. For example:
  ```commandline
  python cli.py analyze-gtfs-into-archive --date-to-analyze
  
  python cli.py analyze-gtfs-into-archive --date-to-analyze 2020-5-5
  ```
  in case there is no GTFS data for that date, and error will be raised:  
  `Can't find relevant gtfs files for 2020-05-05 in .data/gtfs_archive/2020/5/5/.gtfs_metadata.json. Please check that 
  you downloaded GTFS files`
  
- **Load analyzed data into database** this operation has optional parameter **--date-to-analyze** that get date 
  (format: %Y-%m-%d). This param by default uses machine current date. For example:
  ```commandline
  python cli.py upload-analyze-gtfs-from-archive
  
  python cli.py upload-analyze-gtfs-from-archive --date-to-upload 2020-5-5
  ```
  in case there is no analyzed GTFS data for that date, and error will be raised: 
  `Can't find relevant route stat file at .data/stat_archive/2020/5/5/route_stats.csv.gz. 
  Please check that you analyze gtfs files first.`
  in case there is no database as configured in SQLALCHEMY_URL env. parameter the following error will raise:
  `could not connect to server: Connection refused`