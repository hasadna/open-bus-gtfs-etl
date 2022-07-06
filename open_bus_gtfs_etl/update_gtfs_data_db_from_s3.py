import os
import json
import datetime
import subprocess
from textwrap import dedent

from open_bus_stride_db.db import get_session
from open_bus_stride_db.model import GtfsData

from . import config, common


def s3api_list_objects(prefix, max_keys):
    return json.loads(subprocess.check_output(
        [
            'aws', '--output', 'json', 's3api', 'list-objects',
            '--bucket', config.OPEN_BUS_STRIDE_PUBLIC_S3_BUCKET_NAME,
            '--prefix', f'{config.GTFS_ARCHIVE_FOLDER}/{prefix}',
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


def iterate_gtfs_s3_valid_dates(last_days=None):
    last_days = common.parse_None(last_days)
    if last_days is not None:
        last_days = int(last_days)
    if last_days:
        start_date = datetime.date.today() - datetime.timedelta(days=last_days)
    else:
        start_date = datetime.date(2022, 1, 16)
    for year in range(start_date.year, datetime.date.today().year+1):
        if s3api_prefix_has_files(f'{year}/'):
            for month in range(start_date.month if year == start_date.year else 1, 13):
                if s3api_prefix_has_files(f'{year}/{month:02d}/'):
                    for day in range(start_date.day if (year == start_date.year and month == start_date.month) else 1, 32):
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
                                yield datetime.date(year, month, day)


def main(last_days=None):
    for date in iterate_gtfs_s3_valid_dates(last_days):
        with get_session() as session:
            gtfs_data_id = None
            if session.query(GtfsData).filter(GtfsData.date == date, GtfsData.upload_success == True).count() < 1:
                print(f'{date}: upload_success=True')
                gtfs_data = GtfsData(date=date, upload_success=True)
                session.add(gtfs_data)
                session.commit()
                gtfs_data_id = gtfs_data.id
            num_ride_stops = list(session.execute(dedent(f"""
                select count(1)
                from gtfs_ride_stop, gtfs_stop, gtfs_ride, gtfs_route
                where gtfs_ride_stop.gtfs_stop_id = gtfs_stop.id
                and gtfs_ride_stop.gtfs_ride_id = gtfs_ride.id
                and gtfs_ride.gtfs_route_id = gtfs_route.id
                and gtfs_stop.date = '{date.strftime("%Y-%m-%d")}' and gtfs_route.date = '{date.strftime("%Y-%m-%d")}'
            """)))[0][0]
            if num_ride_stops > 1000:
                print(f'{date}: processing_success=True')
                if gtfs_data_id:
                    session.execute(dedent(f"""
                        update gtfs_data set processing_success = true where id = '{gtfs_data_id}'
                    """))
                else:
                    session.add(GtfsData(date=date, processing_success=True))
                session.commit()
