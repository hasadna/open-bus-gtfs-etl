from pathlib import Path
from pprint import pprint
from textwrap import dedent
from collections import defaultdict

from open_bus_stride_db.db import session_decorator, Session
from open_bus_stride_db import model

from . import common, config, partridge_helper


def parse_stop_desc(stop_desc, stats):
    # רחוב: בן יהודה 74 עיר: כפר סבא רציף:  קומה:
    try:
        return stop_desc.split('עיר:')[1].split('רציף:')[0].strip()
    except Exception:
        stats['rows failed to parse stop_desc'] += 1
        return None


@session_decorator
def main(session: Session, date: str, silent=False, extracted_workdir=None):
    date = common.parse_date_str(date)
    dated_workdir = extracted_workdir if extracted_workdir else common.get_dated_workdir(date)
    stats = defaultdict(int)
    with common.print_memory_usage("Preparing partridge feed...", silent=silent):
        feed = partridge_helper.prepare_partridge_feed(
            date, Path(dated_workdir, config.WORKDIR_ISRAEL_PUBLIC_TRANSPORTATION)
        )
    with common.print_memory_usage('Getting all stops from DB...', silent=silent):
        gtfs_stops_by_code = {
            int(gtfs_stop.code): gtfs_stop
            for gtfs_stop
            in session.query(model.GtfsStop).where(model.GtfsStop.date == date).all()
        }
        stats['existing stops in DB'] = len(gtfs_stops_by_code)
    with common.print_memory_usage('Getting all mot_ids from DB...', silent=silent):
        mot_ids_by_code = {}
        for stop_code, mot_id in session.execute(dedent("""
            select s.code, m.mot_id
            from gtfs_stop_mot_id m, gtfs_stop s
            where m.gtfs_stop_id = s.id
            and s.date = '{}'
        """.format(date.strftime('%Y-%m-%d')))).fetchall():
            mot_ids_by_code.setdefault(int(stop_code), set()).add(int(mot_id))
        stats['existing stops with mot ids in DB'] = len(mot_ids_by_code)
    with common.print_memory_usage('Upserting data...', silent=silent):
        for row in feed.stops[[
            'stop_id', 'stop_name', 'stop_lat', 'stop_lon', 'stop_code', 'stop_desc'
        ]].to_dict('records'):
            stats['total rows in source data'] += 1
            stop_id = int(row['stop_id'])
            stop_code = int(row['stop_code'])
            stop_city = parse_stop_desc(row['stop_desc'], stats)
            if stop_code in gtfs_stops_by_code:
                stats['rows updated in DB'] += 1
                gtfs_stop = gtfs_stops_by_code[stop_code]
                gtfs_stop.lat = row['stop_lat']
                gtfs_stop.lon = row['stop_lon']
                gtfs_stop.name = row['stop_name']
                gtfs_stop.city = stop_city
            else:
                stats['rows inserted to DB'] += 1
                gtfs_stop = model.GtfsStop(
                    date=date,
                    code=stop_code,
                    lat=row['stop_lat'],
                    lon=row['stop_lon'],
                    name=row['stop_name'],
                    city=stop_city
                )
                session.add(gtfs_stop)
            if stop_code in mot_ids_by_code:
                if stop_id not in mot_ids_by_code[stop_code]:
                    stats['stop mot id rows inserted to DB'] += 1
                    mot_ids_by_code[stop_code].add(stop_id)
                    session.add(model.GtfsStopMotId(gtfs_stop=gtfs_stop, mot_id=stop_id))
            else:
                stats['stop mot id rows inserted to DB'] += 1
                mot_ids_by_code[stop_code] = {stop_id}
                session.add(model.GtfsStopMotId(gtfs_stop=gtfs_stop, mot_id=stop_id))
    with common.print_memory_usage('Committing...', silent=silent):
        session.commit()
    if not silent:
        pprint(dict(stats))
    return stats
