import datetime
import os
import zipfile
from collections import defaultdict

from open_bus_stride_db import model
from open_bus_stride_db.db import session_decorator, Session

from open_bus_gtfs_etl.api import parse_date_str
from open_bus_gtfs_etl.config import GTFS_ETL_ROOT_ARCHIVES_FOLDER


def parse_time(timestr):
    return list(map(int, timestr.split(':')))


def list_(date, limit):
    date = parse_date_str(date)
    with zipfile.ZipFile(os.path.join(GTFS_ETL_ROOT_ARCHIVES_FOLDER, 'gtfs_archive',
                                      date.strftime('%Y/%m/%d'),
                                      'israel-public-transportation.zip')) as zf:
        with zf.open('stop_times.txt') as f:
            header = None
            num_rows = 0
            for line in f:
                line = line.strip().replace(b"\xef\xbb\xbf", b"").decode()
                line = line.split(',')
                if header is None:
                    header = line
                else:
                    try:
                        row = dict(zip(header, line))
                        row['stop_id'] = int(row['stop_id'])
                        row['stop_sequence'] = int(row['stop_sequence'])
                        row['pickup_type'] = int(row['pickup_type'])
                        row['drop_off_type'] = int(row['drop_off_type'])
                        row['shape_dist_traveled'] = int(row['shape_dist_traveled']) \
                            if row['shape_dist_traveled'] else 0
                        row['arrival_time'] = parse_time(row.pop('arrival_time'))
                        row['departure_time'] = parse_time(row.pop('departure_time'))
                    except Exception:
                        print("Failed to parse line: {}".format(line))
                        raise
                    yield row
                    num_rows += 1
                if limit and num_rows >= limit:
                    break


class ObjectsMaker:

    def __init__(self, stats, session, date):
        self._stats = stats
        self._session = session
        self._date = date
        self._rides_cache = {}
        self._rides_index = []
        self._stops_cache = {}

    def get_ride(self, trip_id):
        if trip_id not in self._rides_cache:
            if len(self._rides_index) > 1000:
                del self._rides_cache[self._rides_index.pop(0)]
            self._rides_index.append(trip_id)
            rides = self._session.query(model.Ride).filter(model.Ride.journey_ref == trip_id,
                                                           model.Ride.is_from_gtfs is True).order_by(
                model.Ride.scheduled_start_time).all()
            if len(rides) == 0:
                self._stats['no rides for trip_id'] += 1
                self._rides_cache[trip_id] = False
            else:
                if len(rides) > 1:
                    self._stats['too many rides for trip_id'] += 1
                self._rides_cache[trip_id] = rides[0]
        return self._rides_cache[trip_id]

    def get_stop(self, stop_id):
        if stop_id not in self._stops_cache:
            stops = self._session.query(model.Stop).filter(model.Stop.code == stop_id,
                                                           model.Stop.is_from_gtfs is True,
                                                           model.Stop.min_date <= self._date,
                                                           self._date <= model.Stop.max_date).order_by(
                model.Stop.id).all()
            if len(stops) == 0:
                self._stats['no stops for stop_id'] += 1
                self._stops_cache[stop_id] = False
            else:
                if len(stops) > 1:
                    self._stats['too many stops for stop_id'] += 1
                self._stops_cache[stop_id] = stops[0]
        return self._stops_cache[stop_id]


@session_decorator
def load_to_db(session: Session, date, limit, no_count):
    stats = defaultdict(int)
    date = parse_date_str(date)
    if no_count:
        count = 9999999999
        print("Skipping counting of stop_times")
    else:
        print("Counting stop_times...")
        count = 0
        for _ in list_(date, limit):
            count += 1
        print("{} stop_times to process".format(count))
    objects_maker = ObjectsMaker(stats, session, date)
    start_time = datetime.datetime.now()
    for i, stop_time in enumerate(list_(date, limit)):
        if i % 10000 == 9999:
            print('{}s: {} / {} ({}%)'.format((datetime.datetime.now() - start_time).total_seconds(), i, count,
                                              i / count * 100))
            print(dict(stats))
        try:
            ride = objects_maker.get_ride(stop_time['trip_id'])
            stop = objects_maker.get_stop(stop_time['stop_id'])
            if not ride or not stop:
                continue
            ride_stop = session.query(model.RideStop).filter(model.RideStop.ride == ride,
                                                             model.RideStop.stop == stop,
                                                             model.RideStop.is_from_gtfs is True).one_or_none()
            if not ride_stop:
                stats['created new ride_stop'] += 1
                session.add(model.RideStop(
                    ride=ride, stop=stop,
                    is_from_gtfs=True,
                    gtfs_arrival_time='{}:{}:{}'.format(*stop_time['arrival_time']),
                    gtfs_departure_time='{}:{}:{}'.format(*stop_time['departure_time']),
                    gtfs_stop_sequence=stop_time['stop_sequence'],
                    gtfs_pickup_type=stop_time['pickup_type'],
                    gtfs_drop_off_type=stop_time['drop_off_type'],
                    gtfs_shape_dist_traveled=stop_time['shape_dist_traveled']
                ))
            else:
                stats['updated existing ride_stop'] += 1
                ride_stop.gtfs_arrival_time = '{}:{}:{}'.format(*stop_time['arrival_time'])
                ride_stop.gtfs_departure_time = '{}:{}:{}'.format(*stop_time['departure_time'])
                ride_stop.gtfs_stop_sequence = stop_time['stop_sequence']
                ride_stop.gtfs_pickup_type = stop_time['pickup_type']
                ride_stop.gtfs_drop_off_type = stop_time['drop_off_type']
                ride_stop.gtfs_shape_dist_traveled = stop_time['shape_dist_traveled']
        except Exception:
            print("Failed to load stop_time to db: {}".format(stop_time))
            raise
    session.commit()
    return dict(stats)
