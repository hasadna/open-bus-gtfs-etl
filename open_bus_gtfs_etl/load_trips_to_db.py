from pathlib import Path
from pprint import pprint
from collections import defaultdict

from open_bus_stride_db.db import session_decorator, Session
from open_bus_stride_db import model

from . import common, config, partridge_helper


@session_decorator
def main(session: Session, date: str, silent=False, extracted_workdir=None):
    date = common.parse_date_str(date)
    dated_workdir = extracted_workdir if extracted_workdir else common.get_dated_workdir(date)
    stats = defaultdict(int)
    with common.print_memory_usage("Preparing partridge feed...", silent=silent):
        feed = partridge_helper.prepare_partridge_feed(
            date, Path(dated_workdir, config.WORKDIR_ISRAEL_PUBLIC_TRANSPORTATION)
        )
    with common.print_memory_usage('Getting all rides from DB...', silent=silent):
        gtfs_rides_by_journey_ref = {
            gtfs_ride.journey_ref: gtfs_ride
            for gtfs_ride
            in session.query(model.GtfsRide).join(model.GtfsRoute.gtfs_rides).where(model.GtfsRoute.date == date).all()
        }
        stats['existing rides loaded from DB'] = len(gtfs_rides_by_journey_ref)
    with common.print_memory_usage('Getting all routes from DB...', silent=silent):
        gtfs_routes_by_line_ref = {
            int(gtfs_route.line_ref): gtfs_route
            for gtfs_route
            in session.query(model.GtfsRoute).where(model.GtfsRoute.date == date).all()
        }
        stats['existing routes loaded from DB'] = len(gtfs_routes_by_line_ref)
    with common.print_memory_usage('Upserting data...', silent=silent):
        for row in feed.trips[['route_id', 'trip_id']].to_dict('records'):
            stats['total rows in source data'] += 1
            route_id = int(row['route_id'])
            trip_id = row['trip_id']
            gtfs_route = gtfs_routes_by_line_ref.get(route_id)
            if gtfs_route:
                if trip_id in gtfs_rides_by_journey_ref:
                    stats['rows updated in DB'] += 1
                    gtfs_ride = gtfs_rides_by_journey_ref[trip_id]
                    gtfs_ride.gtfs_route_id = gtfs_route.id
                else:
                    stats['rows inserted to DB'] += 1
                    session.add(model.GtfsRide(
                        gtfs_route_id=gtfs_route.id,
                        journey_ref=trip_id
                    ))
            else:
                stats['rows missing gtfs route in DB'] += 1
    with common.print_memory_usage('Committing...', silent=silent):
        session.commit()
    if not silent:
        pprint(dict(stats))
    return stats
