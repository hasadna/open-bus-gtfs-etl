import json
import datetime
import traceback
from pathlib import Path
from pprint import pprint
from textwrap import dedent
from collections import defaultdict

import pytz
import numpy
import kvfile
import gtfs_kit

from open_bus_stride_db.db import get_session
from open_bus_stride_db import model

from . import common, config, partridge_helper


def parse_gtfs_datetime(gtfs_time, date, stats, debug):
    try:
        timestr = gtfs_kit.helpers.timestr_to_seconds(float(gtfs_time), inverse=True)
        hours, minutes, seconds = map(int, timestr.split(':'))
        while hours > 23:
            hours -= 24
            date = date + datetime.timedelta(days=1)
        timestr = '{}:{}:{}'.format(str(hours).zfill(2), str(minutes).zfill(2), str(seconds).zfill(2))
        return pytz.timezone('Israel').localize(datetime.datetime.strptime(
            '{} {}'.format(date.strftime('%Y-%m-%d'), timestr),
            '%Y-%m-%d %H:%M:%S'
        ))
    except Exception:
        if debug:
            stats['failed to parse gtfs_time'] += 1
            print("Failed to parse gtfs time: {}".format(gtfs_time))
            traceback.print_exc()
            return None
        else:
            raise


def main(date: str, workdir: str, limit: int, debug: bool):
    date = common.parse_date_str(date)
    workdir = common.get_workdir(workdir)
    stats = defaultdict(int)
    with get_session() as session:
        with common.print_memory_usage('Getting all mot_ids from DB...'):
            gtfs_stop_id_by_mot_ids = {
                mot_id: gtfs_stop_id
                for gtfs_stop_id, mot_id
                in session.execute(dedent("""
                    select s.id, m.mot_id
                    from gtfs_stop_mot_id m, gtfs_stop s
                    where m.gtfs_stop_id = s.id
                    and s.date = '{}'
                """.format(date.strftime('%Y-%m-%d')))).fetchall()
            }
            stats['existing mot ids loaded from DB'] = len(gtfs_stop_id_by_mot_ids)
        with common.print_memory_usage('Getting all route and ride ids from DB...'):
            gtfs_route_ids_ride_ids_by_journey_ref = {
                gtfs_ride.journey_ref: (gtfs_ride.gtfs_route_id, gtfs_ride.id)
                for gtfs_ride
                in session.query(model.GtfsRide).join(model.GtfsRoute).where(model.GtfsRoute.date == date).all()
            }
    with common.print_memory_usage("Preparing partridge feed..."):
        feed = partridge_helper.prepare_partridge_feed(
            date, Path(workdir, config.WORKDIR_ISRAEL_PUBLIC_TRANSPORTATION)
        )
    print("Preparing data for quick loading from disk...")
    rownums_by_route_id = {}
    assert kvfile.db_kind == 'LevelDB', "If not using LevelDB operation is very slow!"
    kv = kvfile.KVFile()
    stop_times = feed.stop_times
    if limit:
        stop_times = stop_times.head(limit)
    for rownum, row in enumerate(stop_times.to_dict('records')):
        if debug or rownum % 10000 == 0:
            print('rownum {}'.format(rownum))
        trip_id = row['trip_id']
        gtfs_route_id_ride_id = gtfs_route_ids_ride_ids_by_journey_ref.get(trip_id)
        if gtfs_route_id_ride_id:
            gtfs_route_id, gtfs_ride_id = gtfs_route_id_ride_id
            if gtfs_route_id and gtfs_ride_id:
                rownums_by_route_id.setdefault(gtfs_route_id, set()).add(rownum)
                stop_id = int(row['stop_id'])
                output_row = dict(
                    arrival_time=parse_gtfs_datetime(row['arrival_time'], date, stats, debug).strftime('%Y-%m-%d %H:%M:%S %z'),
                    departure_time=parse_gtfs_datetime(row['departure_time'], date, stats, debug).strftime('%Y-%m-%d %H:%M:%S %z'),
                    stop_id=int(stop_id),
                    stop_sequence=int(row['stop_sequence']),
                    pickup_type=int(row['pickup_type']),
                    drop_off_type=int(row['drop_off_type']),
                    gtfs_stop_id=gtfs_stop_id_by_mot_ids.get(stop_id),
                    gtfs_ride_id=int(gtfs_ride_id),
                    trip_id=int(trip_id),
                )
                if not row['shape_dist_traveled'] or numpy.isnan(row['shape_dist_traveled']):
                    output_row['shape_dist_traveled'] = None
                else:
                    try:
                        output_row['shape_dist_traveled'] = int(row['shape_dist_traveled'])
                    except Exception:
                        if debug:
                            stats['failed to parse shape_dist_traveled'] += 1
                            output_row['shape_dist_traveled'] = None
                            print("Failed to parse shape_dist_traveled: {}".format(row['shape_dist_traveled']))
                            traceback.print_exc()
                        else:
                            raise
                kv.set(str(rownum), json.dumps(output_row))
    i = 0
    for gtfs_route_id, rownums in rownums_by_route_id.items():
        i += 1
        print("Processing gtfs_route_id {} ({}/{})".format(gtfs_route_id, i, len(rownums_by_route_id)))
        with get_session() as session:
            with common.print_memory_usage('Getting all ride_stops from DB...'):
                gtfs_ride_stops_by_gtfs_ride_id_gtfs_stop_id = {
                    '{}-{}'.format(gtfs_ride_stop.gtfs_ride_id, gtfs_ride_stop.gtfs_stop_id): gtfs_ride_stop
                    for gtfs_ride_stop
                    in session.query(model.GtfsRideStop).join(model.GtfsRide).where(model.GtfsRide.gtfs_route_id == gtfs_route_id).all()
                }
            with common.print_memory_usage('Upserting data...'):
                for rownum in rownums:
                    row = json.loads(kv.get(str(rownum)))
                    row['arrival_time'] = datetime.datetime.strptime(row['arrival_time'], '%Y-%m-%d %H:%M:%S %z')
                    row['departure_time'] = datetime.datetime.strptime(row['departure_time'], '%Y-%m-%d %H:%M:%S %z')
                    gtfs_ride_stop = gtfs_ride_stops_by_gtfs_ride_id_gtfs_stop_id.get('{}-{}'.format(row['gtfs_ride_id'], row['gtfs_stop_id']))
                    if gtfs_ride_stop:
                        stats['rows updated in DB'] += 1
                        gtfs_ride_stop.arrival_time = row['arrival_time']
                        gtfs_ride_stop.departure_time = row['departure_time']
                        gtfs_ride_stop.stop_sequence = row['stop_sequence']
                        gtfs_ride_stop.pickup_type = row['pickup_type']
                        gtfs_ride_stop.drop_off_type = row['drop_off_type']
                        gtfs_ride_stop.shape_dist_traveled = row['shape_dist_traveled']
                    else:
                        stats['rows inserted to DB'] += 1
                        session.add(model.GtfsRideStop(
                            gtfs_ride_id=row['gtfs_ride_id'],
                            gtfs_stop_id=row['gtfs_stop_id'],
                            arrival_time=row['arrival_time'],
                            departure_time=row['departure_time'],
                            stop_sequence=row['stop_sequence'],
                            pickup_type=row['pickup_type'],
                            drop_off_type=row['drop_off_type'],
                            shape_dist_traveled=row['shape_dist_traveled'],
                        ))
            pprint(dict(stats))
            with common.print_memory_usage('Committing...'):
                session.commit()
    pprint(dict(stats))
