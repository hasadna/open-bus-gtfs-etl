import datetime
from pprint import pprint
from collections import defaultdict

from . import (
    common,
    download_extract_upload,
    load_stops_to_db,
    load_routes_to_db,
    load_trips_to_db,
    load_stop_times_to_db,
    cleanup_dated_paths
)


def load_missing_data(dt):
    print("Loading missing data for date {}".format(dt))
    stats = defaultdict(int)
    start_time = datetime.datetime.now()
    try:
        download_extract_upload.main(from_stride=True, date=dt, force_download=True, silent=True)
        load_stops_stats = load_stops_to_db.main(dt, silent=True)
        stats['stop rows updated in DB'] += load_stops_stats['rows updated in DB']
        stats['stop rows inserted to DB'] += load_stops_stats['rows inserted to DB']
        stats['stop mot id rows inserted to DB'] += load_stops_stats['stop mot id rows inserted to DB']
        load_routes_stats = load_routes_to_db.main(dt, silent=True)
        stats['route rows updated in DB'] += load_routes_stats['rows updated in DB']
        stats['route rows insert to DB'] += load_routes_stats['rows inserted to DB']
        load_trips_stats = load_trips_to_db.main(dt, silent=True)
        stats['load trip rows updated in DB'] += load_trips_stats['rows updated in DB']
        stats['load trip rows inserted to DB'] += load_trips_stats['rows inserted to DB']
        load_stop_times_stats = load_stop_times_to_db.main(date=dt, limit=0, debug=False, silent=True)
        stats['stop time rows updated in DB'] += load_stop_times_stats['rows updated in DB']
        stats['stop time rows inserted to DB'] += load_stop_times_stats['rows inserted to DB']
        stats['processed dates'] += 1
    finally:
        print("Elapsed time: {} seconds".format((datetime.datetime.now() - start_time).total_seconds()))
        pprint(dict(stats))
        cleanup_dated_paths.main(10, 10, silent=True)


def main(from_date, to_date):
    from_date = common.parse_date_str(from_date)
    to_date = common.parse_date_str(to_date)
    if to_date > from_date:
        dt = to_date
        min_dt = from_date
    else:
        dt = from_date
        min_dt = to_date
    while dt >= min_dt:
        print(dt)
        load_missing_data(dt.strftime('%Y-%m-%d'))
        dt = dt - datetime.timedelta(days=1)
