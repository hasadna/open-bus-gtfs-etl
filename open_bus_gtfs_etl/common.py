import os
import shutil
import tempfile
import datetime
from pathlib import Path
from contextlib import contextmanager

import psutil
import requests

from . import config


@contextmanager
def safe_open_write(filename, *args, **kwargs):
    with tempfile.TemporaryDirectory() as tempdir:
        temp_filename = os.path.join(tempdir, "file")
        with open(temp_filename, *args, **kwargs) as f:
            yield f
        shutil.move(temp_filename, filename)


def http_stream_download(filename, **requests_kwargs):
    with requests.get(stream=True, **requests_kwargs) as res:
        res.raise_for_status()
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with safe_open_write(filename, 'wb') as f:
            for chunk in res.iter_content(chunk_size=8192):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)


def parse_date_str(date):
    """Parses a date string in format %Y-%m-%d with default of today if empty"""
    if isinstance(date, datetime.date):
        return date
    if not date:
        return datetime.date.today()
    return datetime.datetime.strptime(date, '%Y-%m-%d').date()


def parse_None(val):
    # due to a problem with airflow dag initialization, in some cases we get
    # the actual string 'None' which we need to handle as None
    if val is None or val == 'None':
        return None
    else:
        return val


def get_dated_workdir(date):
    return os.path.join(config.GTFS_ETL_ROOT_ARCHIVES_FOLDER, 'workdir', date.strftime('%Y/%m/%d'))


def get_dated_path(date, *args):
    return Path(config.GTFS_ETL_ROOT_ARCHIVES_FOLDER, config.GTFS_ARCHIVE_FOLDER).joinpath(date.strftime('%Y/%m/%d'), *args)


def get_s3_dated_path(date, *args):
    return os.path.join(f's3://{config.OPEN_BUS_STRIDE_PUBLIC_S3_BUCKET_NAME}/{config.GTFS_ARCHIVE_FOLDER}/{date.strftime("%Y/%m/%d")}', *args)


@contextmanager
def print_memory_usage(start_msg, end_msg="Done", silent=False):
    if not silent:
        print(start_msg)
    yield
    if not silent:
        print("{}. Resident memory: {}mb".format(end_msg, psutil.Process().memory_info().rss / (1024 * 1024)))


class UserError(Exception):
    """
    Exception that represent error caused by wrong input of user
    """
