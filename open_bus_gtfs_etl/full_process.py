import tempfile
import datetime
from pprint import pprint
from collections import defaultdict

from open_bus_stride_db.db import get_session
from open_bus_stride_db.model import GtfsData

from . import common, download_extract_upload, load_stops_to_db, load_trips_to_db, load_routes_to_db, load_stop_times_to_db


# the last days for which we want to make sure all data exists
DEFAULT_LAST_DAYS = 30

# this will not stop a processing in progress, only once a date finished processing it may stop
# allowing the task scheduler to handle the next date
DEFAULT_MAX_RUN_TIME_SECONDS = 60 * 55


def iterate_last_dates(last_days):
    for minus_days in range(last_days+1):
        is_today = minus_days == 0
        yield is_today, (datetime.date.today() - datetime.timedelta(days=minus_days))


def create_gtfs_data_processing_started(date):
    with get_session() as session:
        gtfs_data = GtfsData(
            date=date,
            processing_started_at=datetime.datetime.now(datetime.timezone.utc),
        )
        session.add(gtfs_data)
        session.commit()
        return gtfs_data.id


def download_upload_from_mot(workdir, date, stats):
    print(f"Starting download / upload from MOT for date {date}")
    stats['download_upload_from_mot'] += 1
    return download_extract_upload.main(from_mot=True, date=date, target_path=workdir)


def download_upload_from_stride(workdir, date, from_stride_date, stats):
    print(f"Starting download / upload from Stride for date {date} from stride date {from_stride_date}")
    stats['download_upload_from_stride'] += 1
    return download_extract_upload.main(from_stride=True, date=from_stride_date, target_path=workdir, upload_date=date)


def download_extract(workdir, date, stats):
    print(f"Starting download / extract for date {date}")
    stats['download_extract'] += 1
    return download_extract_upload.main(from_stride=True, date=date, target_path=workdir)


def update_gtfs_data_processing_failed(gtfs_data_id, error):
    with get_session() as session:
        gtfs_data = session.query(GtfsData).filter(GtfsData.id == gtfs_data_id).one()
        gtfs_data.processing_completed_at = datetime.datetime.now(datetime.timezone.utc)
        gtfs_data.processing_error = error
        session.commit()


def update_gtfs_data_upload_success(gtfs_data_id):
    with get_session() as session:
        gtfs_data = session.query(GtfsData).filter(GtfsData.id == gtfs_data_id).one()
        gtfs_data.upload_success = True
        session.commit()


def process_gtfs_data(extracted_workdir, date, stats):
    print(f"Starting processing GTFS data for date {date}")
    stats['process_gtfs_data'] += 1
    load_stops_stats = load_stops_to_db.main(date, silent=True, extracted_workdir=extracted_workdir)
    stats['stop rows updated in DB'] += load_stops_stats['rows updated in DB']
    stats['stop rows inserted to DB'] += load_stops_stats['rows inserted to DB']
    stats['stop mot id rows inserted to DB'] += load_stops_stats['stop mot id rows inserted to DB']
    load_routes_stats = load_routes_to_db.main(date, silent=True, extracted_workdir=extracted_workdir)
    stats['route rows updated in DB'] += load_routes_stats['rows updated in DB']
    stats['route rows insert to DB'] += load_routes_stats['rows inserted to DB']
    load_trips_stats = load_trips_to_db.main(date, silent=True, extracted_workdir=extracted_workdir)
    stats['load trip rows updated in DB'] += load_trips_stats['rows updated in DB']
    stats['load trip rows inserted to DB'] += load_trips_stats['rows inserted to DB']
    load_stop_times_stats = load_stop_times_to_db.main(date=date, limit=0, debug=False, silent=True, extracted_workdir=extracted_workdir)
    stats['stop time rows updated in DB'] += load_stop_times_stats['rows updated in DB']
    stats['stop time rows inserted to DB'] += load_stop_times_stats['rows inserted to DB']


def update_gtfs_data_processing_success(gtfs_data_id):
    with get_session() as session:
        gtfs_data = session.query(GtfsData).filter(GtfsData.id == gtfs_data_id).one()
        gtfs_data.processing_completed_at = datetime.datetime.now(datetime.timezone.utc)
        gtfs_data.processing_success = True
        session.commit()


def process_date_check_missing_upload_data(is_today, date):
    needs_upload_from_mot = False
    needs_upload_from_stride_date = None
    with get_session() as session:
        if session.query(GtfsData).filter(GtfsData.date == date, GtfsData.upload_success == True).count() < 1:
            print(f'Uploaded data is missing for date {date}')
            if is_today:
                print("Date is today, will get the data from MOT")
                needs_upload_from_mot = True
            else:
                gtfs_data = session.query(GtfsData).filter(GtfsData.date > date, GtfsData.upload_success == True) \
                    .order_by(GtfsData.date).limit(1).one_or_none()
                assert gtfs_data, f'Failed to find GTFS data with upload_success after date {date}'
                assert (gtfs_data.date - date) > datetime.timedelta(days=5), \
                    f'Found GTFS data with upload_success after date {date} but it is more than 5 days away'
                print(f"Date is not today, will get the data from stride date {gtfs_data.date}")
                needs_upload_from_stride_date = gtfs_data.date
    return needs_upload_from_mot, needs_upload_from_stride_date


def process_date_handle_upload(needs_upload_from_mot, needs_upload_from_stride_date, date, workdir, stats):
    extracted_workdir = None
    needs_processing = False
    gtfs_data_id = None
    if needs_upload_from_mot or needs_upload_from_stride_date:
        needs_processing = True
        gtfs_data_id = create_gtfs_data_processing_started(date)
        print(f"Created gtfs_data id {gtfs_data_id}")
        try:
            if needs_upload_from_mot:
                extracted_workdir = download_upload_from_mot(workdir, date, stats)
            elif needs_upload_from_stride_date:
                extracted_workdir = download_upload_from_stride(workdir, date, needs_upload_from_stride_date, stats)
        except Exception as e:
            update_gtfs_data_processing_failed(gtfs_data_id, str(e))
            raise
        else:
            update_gtfs_data_upload_success(gtfs_data_id)
    return gtfs_data_id, extracted_workdir, needs_processing


def process_date_check_needs_processing(needs_processing, date):
    if not needs_processing:
        with get_session() as session:
            if session.query(GtfsData).filter(GtfsData.date == date, GtfsData.processing_success == True).count() < 1:
                print(f"Processing was not completed for date {date}")
                needs_processing = True
    return needs_processing


def process_date_handle_processing(needs_processing, gtfs_data_id, date, extracted_workdir, stats, workdir):
    if needs_processing:
        if not gtfs_data_id:
            gtfs_data_id = create_gtfs_data_processing_started(date)
            print(f"Created gtfs_data id {gtfs_data_id}")
        try:
            if not extracted_workdir:
                extracted_workdir = download_extract(workdir, date, stats)
            process_gtfs_data(extracted_workdir, date, stats)
        except Exception as e:
            update_gtfs_data_processing_failed(gtfs_data_id, str(e))
            raise
        else:
            update_gtfs_data_processing_success(gtfs_data_id)


def process_date(is_today, date, stats):
    needs_upload_from_mot, needs_upload_from_stride_date = process_date_check_missing_upload_data(is_today, date)
    with tempfile.TemporaryDirectory() as workdir:
        gtfs_data_id, extracted_workdir, needs_processing \
            = process_date_handle_upload(needs_upload_from_mot, needs_upload_from_stride_date, date, workdir, stats)
        needs_processing = process_date_check_needs_processing(needs_processing, date)
        process_date_handle_processing(needs_processing, gtfs_data_id, date, extracted_workdir, stats, workdir)


def main(last_days=None, max_run_time_seconds=None, only_date=None):
    """This task is supposed to be idempotent and make sure that all GTFS data
    is available for last_days days. It uses DB gtfs_data table to keep track
    of the days for which we have GTFS data.
    """
    stats = defaultdict(int)
    last_days = common.parse_None(last_days)
    max_run_time_seconds = common.parse_None(max_run_time_seconds)
    only_date = common.parse_None(only_date)
    if only_date:
        only_date = common.parse_date_str(only_date)
        is_today = only_date == datetime.date.today()
        process_date(is_today, only_date, {})
        stats['processed_dates'] += 1
    else:
        if not last_days:
            last_days = DEFAULT_LAST_DAYS
        if not max_run_time_seconds:
            max_run_time_seconds = DEFAULT_MAX_RUN_TIME_SECONDS
        last_days = int(last_days)
        max_run_time_seconds = int(max_run_time_seconds)
        start_time = datetime.datetime.now(datetime.timezone.utc)
        for is_today, date in iterate_last_dates(last_days):
            process_date(is_today, date, stats)
            stats['processed_dates'] += 1
            if (datetime.datetime.now(datetime.timezone.utc) - start_time) > datetime.timedelta(seconds=max_run_time_seconds):
                print(f"Max run time of {max_run_time_seconds} seconds reached, exiting")
                break
    pprint(dict(stats))
