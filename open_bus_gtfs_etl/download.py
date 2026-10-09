import io
import os
import csv
import zipfile
import datetime

from . import common, config, gtfs_extractor


def get_service_date(archive_folder):
    """Returns the date a downloaded MOT GTFS file is for.

    MOT publishes the file for date D on the evening of D-1, and that file's calendar starts
    on D - the earliest calendar.txt start_date is D (verified on archived files 2025-10..2026-10).
    A file still serving the previous day therefore starts a day early.
    """
    with zipfile.ZipFile(os.path.join(archive_folder, 'israel-public-transportation.zip')) as gtfs_zip:
        with gtfs_zip.open('calendar.txt') as f:
            return min(
                datetime.datetime.strptime(row['start_date'], '%Y%m%d').date()
                for row in csv.DictReader(io.TextIOWrapper(f, 'utf-8-sig'))
            )


def from_mot(archive_folder=None, date=None):
    if not date:
        date = common.israel_today()
    if not archive_folder:
        archive_folder = common.get_dated_path(date)
    print("Downloading GTFS files to archive folder: {}".format(archive_folder))
    gtfs_extractor.GtfsRetriever(archive_folder).retrieve_gtfs_files()
    return date


def from_stride(date, force_download, silent=False, archive_folder=None):
    date = common.parse_date_str(date)
    assert date, 'must provide date or download analyzed data'
    s3_path = common.get_s3_path('gtfs_archive', date.strftime("%Y/%m/%d"))
    base_url = f'https://openbus-stride-public.s3.eu-west-1.amazonaws.com/{s3_path}/'
    if not archive_folder:
        base_path = os.path.join(config.GTFS_ETL_ROOT_ARCHIVES_FOLDER, 'gtfs_archive', date.strftime('%Y/%m/%d'))
    else:
        base_path = archive_folder
    if not silent:
        print(f"Downloading GTFS files from {base_url} to {base_path}")
    if date.strftime("%Y-%m-%d") in ('2023-03-26', '2023-03-27', '2023-03-28', '2023-03-29', '2023-03-30', '2023-03-31', '2023-04-01', '2023-04-02'):
        print("WARNING! Only israel-public-transporation.zip is available for given date, other files are missing")
        filenames = ['israel-public-transportation.zip']
    else:
        filenames = ['ClusterToLine.zip', 'Tariff.zip', 'TripIdToDate.zip', 'israel-public-transportation.zip']
    for filename in filenames:
        url = base_url + filename
        path = os.path.join(base_path, filename)
        if os.path.exists(path) and not force_download:
            print("File already exists: {}".format(path))
        else:
            common.http_stream_download(path, url=url)
    return date
