import time
import tempfile
import datetime
import traceback
from pprint import pprint
from collections import defaultdict

from open_bus_stride_db.db import get_session
from open_bus_stride_db.model import GtfsData

from . import common, download_extract_upload, load_stops_to_db, load_trips_to_db, load_routes_to_db, load_stop_times_to_db


# the last days for which we want to make sure all data exists
# our first historical GTFS data is from 2022-01-16 - so we count days since then
EARLIEST_DATA_DATE = datetime.date(2022, 1, 16)
DEFAULT_LAST_DAYS = (datetime.datetime.now(datetime.timezone.utc).date() - EARLIEST_DATA_DATE).days


def iterate_last_dates(last_days):
    for minus_days in range(last_days+1):
        yield datetime.date.today() - datetime.timedelta(days=minus_days)


def download_from_stride(workdir, from_stride_date, stats):
    print(f"Starting download from Stride date {from_stride_date}")
    stats['download_from_stride'] += 1
    return download_extract_upload.main(from_stride=True, date=from_stride_date, target_path=workdir)


def process_gtfs_data(extracted_workdir, date, stats):
    print(f"Processing GTFS data for date {date}...")
    stats['process_gtfs_data'] += 1
    load_stops_stats = load_stops_to_db.main(date, silent=True, extracted_workdir=extracted_workdir)
    print("Loaded stops")
    pprint(dict(load_stops_stats))
    stats['stop rows updated in DB'] += load_stops_stats['rows updated in DB']
    stats['stop rows inserted to DB'] += load_stops_stats['rows inserted to DB']
    stats['stop mot id rows inserted to DB'] += load_stops_stats['stop mot id rows inserted to DB']
    load_routes_stats = load_routes_to_db.main(date, silent=True, extracted_workdir=extracted_workdir)
    print("Loaded routes")
    pprint(dict(load_routes_stats))
    stats['route rows updated in DB'] += load_routes_stats['rows updated in DB']
    stats['route rows insert to DB'] += load_routes_stats['rows inserted to DB']
    load_trips_stats = load_trips_to_db.main(date, silent=True, extracted_workdir=extracted_workdir)
    print("Loaded trips")
    pprint(dict(load_trips_stats))
    stats['load trip rows updated in DB'] += load_trips_stats['rows updated in DB']
    stats['load trip rows inserted to DB'] += load_trips_stats['rows inserted to DB']
    load_stop_times_stats = load_stop_times_to_db.main(date=date, limit=0, debug=False, silent=True, extracted_workdir=extracted_workdir)
    print("Loaded stop times")
    pprint(dict(load_stop_times_stats))
    stats['stop time rows updated in DB'] += load_stop_times_stats['rows updated in DB']
    stats['stop time rows inserted to DB'] += load_stop_times_stats['rows inserted to DB']


def gtfs_data_processing_started(date, processing_used_stride_date=None):
    with get_session() as session:
        gtfs_data = session.query(GtfsData).filter(GtfsData.date == date).one_or_none()
        if gtfs_data is None:
            gtfs_data = GtfsData(
                date=date,
                processing_started_at=datetime.datetime.now(datetime.timezone.utc),
                processing_used_stride_date=processing_used_stride_date,
            )
            session.add(gtfs_data)
            session.commit()
            return gtfs_data.id
        else:
            gtfs_data.processing_started_at = datetime.datetime.now(datetime.timezone.utc)
            gtfs_data.processing_completed_at = None
            gtfs_data.processing_error = None
            gtfs_data.processing_success = None
            gtfs_data.processing_used_stride_date = processing_used_stride_date
            session.commit()
            return gtfs_data.id


def update_gtfs_data(gtfs_data_id, error=None, success=None):
    with get_session() as session:
        gtfs_data = session.query(GtfsData).filter(GtfsData.id == gtfs_data_id).one_or_none()
        assert gtfs_data
        gtfs_data.processing_completed_at = datetime.datetime.now(datetime.timezone.utc)
        if success:
            assert not error
            gtfs_data.processing_success = True
        else:
            assert error
            gtfs_data.processing_error = error
            gtfs_data.processing_success = False
        session.commit()


def check_date(date):
    needs_processing_download_from_stride_date = None
    with get_session() as session:
        if session.query(GtfsData).filter(GtfsData.date == date, GtfsData.processing_success == True).count() < 1:
            gtfs_data = session.query(GtfsData).filter(GtfsData.date <= date, GtfsData.download_upload_success == True) \
                .order_by(GtfsData.date.desc()).limit(1).one_or_none()
            assert gtfs_data, f'Failed to find GTFS data with upload_success on or after date {date}'
            assert (date - gtfs_data.date) <= datetime.timedelta(days=10), \
                f'Found GTFS data with upload_success after date {date} but it is more than 10 days away ({gtfs_data.date})'
            needs_processing_download_from_stride_date = gtfs_data.date
    return needs_processing_download_from_stride_date


def do_process_date(date, stats, download_from_stride_date):
    with tempfile.TemporaryDirectory() as workdir:
        extracted_workdir = download_from_stride(workdir, download_from_stride_date, stats)
        gtfs_data_id = gtfs_data_processing_started(
            date,
            processing_used_stride_date=download_from_stride_date
        )
        try:
            process_gtfs_data(extracted_workdir, date, stats)
        except:
            update_gtfs_data(gtfs_data_id, error=traceback.format_exc())
            raise
        else:
            update_gtfs_data(gtfs_data_id, success=True)


def process_date(date, stats):
    needs_processing_download_from_stride_date = check_date(date)
    if needs_processing_download_from_stride_date:
        print(f'Processing was not completed for date {date}, will download the data from Stride date {needs_processing_download_from_stride_date}')
        do_process_date(date, stats, needs_processing_download_from_stride_date)
        return True
    else:
        return False


def process_iterate_last_dates(last_days, stats):
    for date in iterate_last_dates(last_days):
        if process_date(date, stats):
            stats['processed_dates'] += 1
            return True
    return False


def main(last_days=None, only_date=None):
    """This task is idempotent and makes sure that all GTFS data
    was processed for last_days days. It uses DB gtfs_data table to keep track
    of the days for which we have GTFS data. It has 3 modes of operation:
    1. only_date is set: process only the given date
    2. only_date is not set: iterate over the given last_days and make sure all of them are processed.
                             after a date was processed it starts iterating over all dates again,
                             so that newest dates will always be processed first.
    """
    last_days = common.parse_None(last_days)
    only_date = common.parse_None(only_date)
    stats = defaultdict(int)
    if only_date is not None:
        assert last_days is None
        only_date = common.parse_date_str(only_date)
        process_date(only_date, stats)
        stats['processed_dates'] += 1
    else:
        if not last_days:
            last_days = DEFAULT_LAST_DAYS
        last_days = int(last_days)
        while process_iterate_last_dates(last_days, stats):
            pass
    pprint(dict(stats))
    print('OK')
