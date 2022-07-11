import os
import json
import datetime
import subprocess
from textwrap import dedent

from open_bus_stride_db.db import get_session
from open_bus_stride_db.model import GtfsData

from . import config, common, idempotent_process


def s3api_list_objects(prefix, max_keys):
    return json.loads(subprocess.check_output(
        [
            'aws', '--output', 'json', 's3api', 'list-objects',
            '--bucket', config.OPEN_BUS_STRIDE_PUBLIC_S3_BUCKET_NAME,
            '--prefix', common.get_s3_path(config.GTFS_ARCHIVE_FOLDER, prefix),
            '--max-keys', str(max_keys),
        ],
        env={
            **os.environ,
            'AWS_ACCESS_KEY_ID': config.OPEN_BUS_STRIDE_PUBLIC_S3_WRITE_ACCESS_KEY_ID,
            'AWS_SECRET_ACCESS_KEY': config.OPEN_BUS_STRIDE_PUBLIC_S3_WRITE_SECRET_ACCESS_KEY,
        }
    )).get('Contents', [])


def s3api_prefix_has_files(prefix):
    return len(s3api_list_objects(prefix, 1)) > 0


def validate_s3_date(year, month, day):
    if s3api_prefix_has_files(f'{year}/{month:02d}/{day:02d}/'):
        expected_file_names = {
            'ClusterToLine.zip': False,
            'Tariff.zip': False,
            'TripIdToDate.zip': False,
            'israel-public-transportation.zip': False
        }
        for obj in s3api_list_objects(f'{year}/{month:02d}/{day:02d}/', 1000):
            file_name = obj['Key'].split('/')[-1]
            if file_name in expected_file_names and obj['Size'] > 1000:
                expected_file_names[file_name] = True
        if all(expected_file_names.values()):
            return datetime.date(year, month, day)
    return None


def iterate_gtfs_s3_valid_dates(last_days=None, only_date=None):
    if only_date:
        valid_s3_date = validate_s3_date(only_date.year, only_date.month, only_date.day)
        if valid_s3_date:
            yield valid_s3_date
    else:
        last_days = common.parse_None(last_days)
        if last_days is not None:
            last_days = int(last_days)
        if last_days:
            start_date = datetime.date.today() - datetime.timedelta(days=last_days)
        else:
            start_date = idempotent_process.EARLIEST_DATA_DATE
        for year in range(start_date.year, datetime.date.today().year+1):
            if s3api_prefix_has_files(f'{year}/'):
                for month in range(start_date.month if year == start_date.year else 1, 13):
                    if s3api_prefix_has_files(f'{year}/{month:02d}/'):
                        for day in range(start_date.day if (year == start_date.year and month == start_date.month) else 1, 32):
                            valid_s3_date = validate_s3_date(year, month, day)
                            if valid_s3_date:
                                yield valid_s3_date


def iterate_last_dates(last_days, only_date=None):
    if last_days is None:
        last_days = idempotent_process.DEFAULT_LAST_DAYS
    for minus_days in range(last_days+1):
        date = datetime.date.today() - datetime.timedelta(days=minus_days)
        if only_date is None or date == only_date:
            yield date


def main(last_days=None, only_s3=False, only_db=False, only_date=None):
    with_s3, with_db = True, True
    if only_s3:
        assert not only_db
        with_db = False
    elif only_db:
        with_s3 = False
    only_date = common.parse_None(only_date)
    if only_date is not None:
        only_date = common.parse_date_str(only_date)
    with get_session() as session:
        if with_s3:
            for date in iterate_gtfs_s3_valid_dates(last_days, only_date=only_date):
                gtfs_data = session.query(GtfsData).filter(GtfsData.date == date).one_or_none()
                if gtfs_data is None:
                    print(f'Adding gtfs_data for date {date} with download_upload_success=True')
                    session.add(GtfsData(
                        date=date,
                        download_upload_success=True
                    ))
                    session.commit()
                elif gtfs_data.download_upload_success is None:
                    print(f'Updating gtfs_data for date {date} with download_upload_success=True')
                    gtfs_data.download_upload_started_at = None
                    gtfs_data.download_upload_completed_at = None
                    gtfs_data.download_upload_error = None
                    gtfs_data.download_upload_success = True
                    session.commit()
        if with_db:
            for date in iterate_last_dates(last_days, only_date=only_date):
                gtfs_data = session.query(GtfsData).filter(GtfsData.date == date).one_or_none()
                if gtfs_data is None or gtfs_data.processing_success is None:
                    num_ride_stops = list(session.execute(dedent(f"""
                        select count(1)
                        from gtfs_ride_stop, gtfs_stop, gtfs_ride, gtfs_route
                        where gtfs_ride_stop.gtfs_stop_id = gtfs_stop.id
                        and gtfs_ride_stop.gtfs_ride_id = gtfs_ride.id
                        and gtfs_ride.gtfs_route_id = gtfs_route.id
                        and gtfs_stop.date = '{date.strftime("%Y-%m-%d")}' and gtfs_route.date = '{date.strftime("%Y-%m-%d")}'
                    """)))[0][0]
                    if num_ride_stops > 1000:
                        if gtfs_data is None:
                            print(f'Adding gtfs_data for date {date} with processing_success=True')
                            session.add(GtfsData(
                                date=date,
                                processing_success=True
                            ))
                        else:
                            print(f'Updating gtfs_data for date {date} with processing_success=True')
                            gtfs_data.processing_started_at = None
                            gtfs_data.processing_completed_at = None
                            gtfs_data.processing_error = None
                            gtfs_data.processing_success = True
                        session.commit()
