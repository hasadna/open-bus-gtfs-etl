from pathlib import Path
from pprint import pprint
from collections import defaultdict

from open_bus_stride_db.db import session_decorator, Session
from open_bus_stride_db import model

from . import common, config, partridge_helper


@session_decorator
def main(session: Session, date: str, workdir: str):
    date = common.parse_date_str(date)
    workdir = common.get_workdir(workdir)
    stats = defaultdict(int)
    with common.print_memory_usage("Preparing partridge feed..."):
        feed = partridge_helper.prepare_partridge_feed(
            date, Path(workdir, config.WORKDIR_ISRAEL_PUBLIC_TRANSPORTATION)
        )
    agencies_by_id = {
        int(row['agency_id']): row['agency_name']
        for row in
        feed.agency[['agency_id', 'agency_name']].to_dict('records')
    }
    stats['agencies from source feed'] = len(agencies_by_id)
    with common.print_memory_usage('Getting all routes from DB...'):
        gtfs_routes_by_line_ref = {
            int(gtfs_route.line_ref): gtfs_route
            for gtfs_route
            in session.query(model.GtfsRoute).where(model.GtfsRoute.date == date).all()
        }
        stats['existing routes loaded from DB'] = len(gtfs_routes_by_line_ref)
    with common.print_memory_usage('Upserting data...'):
        for row in feed.routes[[
            'route_id', 'route_short_name', 'route_long_name', 'route_type', 'agency_id', 'route_desc'
        ]].to_dict('records'):
            stats['total rows in source data'] += 1
            route_id = int(row['route_id'])
            agency_id = int(row['agency_id'])
            route_desc = row['route_desc']
            try:
                route_mkt, route_direction, route_alternative = route_desc.split('-')
            except Exception:
                stats['rows failed to parse route_desc'] += 1
                route_mkt, route_direction, route_alternative = None, None, None
            agency_name = agencies_by_id.get(agency_id)
            if route_id in gtfs_routes_by_line_ref:
                stats['rows updated in DB'] += 1
                gtfs_route = gtfs_routes_by_line_ref[route_id]
                gtfs_route.route_short_name = row['route_short_name']
                gtfs_route.route_long_name = row['route_long_name']
                gtfs_route.route_mkt = route_mkt
                gtfs_route.route_direction = route_direction
                gtfs_route.route_alternative = route_alternative
                gtfs_route.agency_name = agency_name
                gtfs_route.route_type = row['route_type']
            else:
                stats['rows inserted to DB'] += 1
                session.add(model.GtfsRoute(
                    date=date,
                    line_ref=route_id,
                    operator_ref=agency_id,
                    route_short_name=row['route_short_name'],
                    route_long_name=row['route_long_name'],
                    route_mkt=route_mkt,
                    route_direction=route_direction,
                    route_alternative=route_alternative,
                    agency_name=agency_name,
                    route_type=row['route_type']
                ))
    with common.print_memory_usage('Committing...'):
        session.commit()
    pprint(dict(stats))
