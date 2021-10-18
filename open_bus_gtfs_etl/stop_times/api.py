import os
import zipfile
import datetime
from collections import defaultdict

from ..api import parse_date_str
from ..config import GTFS_ETL_ROOT_ARCHIVES_FOLDER
from open_bus_stride_db.db import session_decorator, Session
from open_bus_stride_db import model


def parse_datetime(trip_id, timestr):
    datestr = trip_id.split('_')[1]
    date = datetime.datetime.strptime(datestr, '%d%m%y').date()
    hours, minutes, seconds = list(map(int, timestr.split(':')))
    if hours > 23:
        date += datetime.timedelta(days=1)
        hours = hours - 24
    return datetime.datetime.combine(date, datetime.time(hours, minutes, seconds))


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
                        row['shape_dist_traveled'] = int(row['shape_dist_traveled'])
                        row['arrival_datetime'] = parse_datetime(row['trip_id'], row.pop('arrival_time'))
                        row['departure_datetime'] = parse_datetime(row['trip_id'], row.pop('departure_time'))
                    except:
                        print("Failed to parse line: {}".format(line))
                        raise
                    yield row
                    num_rows += 1
                if limit and num_rows >= limit:
                    break


@session_decorator
def load_to_db(session: Session, date, limit):
    stats = defaultdict(int)
    date = parse_date_str(date)
    for i, stop_time in enumerate(list_(date, limit)):
        if i % 10000 == 9999:
            print(dict(stats))
        try:
            rides = session.query(model.Ride).filter(model.Ride.journey_ref == stop_time['trip_id'],
                                                     model.Ride.is_from_gtfs == True).order_by(model.Ride.scheduled_start_time).all()
            if len(rides) == 0:
                stats['stop_time has no rides for trip'] += 1
                # print("WARNING! No rides for trip_id {}".format(stop_time['trip_id']))
                continue
            elif len(rides) > 1:
                stats['stop_time has too many rides for trip'] += 1
                ride = rides[0]
            else:
                ride = rides[0]
            stops = session.query(model.Stop).filter(model.Stop.code == stop_time['stop_id'],
                                                     model.Stop.is_from_gtfs == True,
                                                     model.Stop.min_date <= date,
                                                     date <= model.Stop.max_date).order_by(model.Stop.id).all()
            if len(stops) == 0:
                stats['stop_time has no stops for stop_id'] += 1
                # print("WARNING! No stops for stop_id {}".format(stop_time['stop_id']))
                continue
            elif len(stops) > 1:
                stats['stop_time has too many stops for stop_id'] += 1
                # print("WARNING! Found {} stops for stop_id {}, using first one".format(len(stops), stop_time['stop_id']))
                stop = stops[0]
            else:
                stop = stops[0]
            ride_stop = session.query(model.RideStop).filter(model.RideStop.ride == ride,
                                                             model.RideStop.stop == stop,
                                                             model.RideStop.is_from_gtfs == True).one_or_none()
            if not ride_stop:
                stats['created new ride_stop'] +=1
                session.add(model.RideStop(
                    ride=ride, stop=stop,
                    is_from_gtfs=True,
                    gtfs_arrival_datetime=stop_time['arrival_datetime'],
                    gtfs_departure_datetime=stop_time['departure_datetime'],
                    gtfs_stop_sequence=stop_time['stop_sequence'],
                    gtfs_pickup_type=stop_time['pickup_type'],
                    gtfs_drop_off_type=stop_time['drop_off_type'],
                    gtfs_shape_dist_traveled=stop_time['shape_dist_traveled']
                ))
            else:
                stats['updated existing ride_stop'] += 1
                ride_stop.gtfs_arrival_datetime = stop_time['arrival_datetime']
                ride_stop.gtfs_departure_datetime = stop_time['departure_datetime']
                ride_stop.gtfs_stop_sequence = stop_time['stop_sequence']
                ride_stop.gtfs_pickup_type = stop_time['pickup_type']
                ride_stop.gtfs_drop_off_type = stop_time['drop_off_type']
                ride_stop.gtfs_shape_dist_traveled = stop_time['shape_dist_traveled']
        except:
            print("Failed to load stop_time to db: {}".format(stop_time))
            raise
    session.commit()
    return dict(stats)
