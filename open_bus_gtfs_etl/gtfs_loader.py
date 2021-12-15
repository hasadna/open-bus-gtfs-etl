
from datetime import datetime, timedelta, date

from typing import List, Optional, Dict

import pytz
from open_bus_stride_db.db import session_decorator
from open_bus_stride_db.model import Route, Ride, RouteStop, Stop
from pydantic import BaseModel
from sqlalchemy.orm import Session
from tqdm import tqdm


class StopModel(BaseModel):
    stop_code: int
    stop_lat: float
    stop_lon: float
    stop_name: str
    stop_desc_city: str
    stop_date: date

    @classmethod
    def parse(cls, stop_code: str, stop_name: str, stop_desc_city: str, stop_lat_lon: str, stop_date: date) \
            -> "StopModel":

        lat, lon = stop_lat_lon.split(",")
        return cls(stop_code=int(stop_code), stop_name=stop_name, stop_desc_city=stop_desc_city,
                   stop_lat=lat, stop_lon=lon, stop_date=stop_date)


class RouteRecord(BaseModel):
    """
    RouteRecord represents a row from route stat file
    """
    date: Optional[date]
    route_id: int
    agency_id: int
    route_short_name: str
    route_long_name: str
    route_mkt: int
    route_direction: int
    route_alternative: str
    agency_name: str
    route_type: int
    all_start_time: str
    all_trip_id: str
    all_trip_id_to_date: str
    all_stop_code: str
    all_stop_id: str
    all_stop_name: str
    all_stop_desc_city: str
    all_stop_lat_lon: str

    @classmethod
    def from_row(cls, row):
        return cls(**dict(row.iteritems()))

    def convert_into_db_route(self, stops: Dict[int, Stop]) -> Route:

        return Route(min_date=self.date, max_date=self.date, line_ref=self.route_id,
                     operator_ref=self.agency_id, gtfs_route_short_name=self.route_short_name,
                     gtfs_route_long_name=self.route_long_name, gtfs_route_mkt=self.route_mkt,
                     gtfs_route_direction=self.route_direction,
                     gtfs_route_alternative=self.route_alternative,
                     gtfs_agency_name=self.agency_name,
                     gtfs_route_type=self.route_type, is_from_gtfs=True,
                     rides=self._get_rides(), route_stops=self._get_route_stops(stops))

    def _get_rides(self) -> List[Ride]:

        return [Ride(scheduled_start_time=self._parse_timestr(start_time),
                     journey_ref=trip_id, is_from_gtfs=True)
                for start_time, trip_id, trip_id_to_date
                in zip(self.all_start_time.split(";"),
                       self.all_trip_id.split(";"),
                       self.all_trip_id_to_date.split(";"))]

    def _get_route_stops(self, stops: Dict[int, Stop]) -> List[RouteStop]:
        """
        creates route stops items based on the route stat information.
        Args:
            stops: existing Stops

        Returns: list of RouteStops

        """
        res = []

        for ind, (stop_code, stop_name, stop_desc_city, stop_lat_lon) \
                in enumerate(zip(self.all_stop_code.split(";"), self.all_stop_name.split(";"),
                                 self.all_stop_desc_city.split(";"), self.all_stop_lat_lon.split(";")), start=1):

            stop_to_upsert = StopModel.parse(stop_code, stop_name, stop_desc_city, stop_lat_lon, self.date)
            stop = _upsert_stop(stops=stops, stop_to_upsert=stop_to_upsert)
            res.append(RouteStop(is_from_gtfs=True, order=ind,
                                 stop=stop))
        return res

    def _parse_timestr(self, time_str: str):
        return datetime.combine(self.date,
                                datetime.strptime(time_str, '%H:%M:%S').time(),
                                tzinfo=pytz.UTC)


def _upsert_stop(stops: Dict[int, Stop], stop_to_upsert: StopModel) -> Stop:
    """
    Upsert (Update or Insert) Stops. Based on given dict of stops that represent the current up-to-date stops,
    this method validate that new Stop will be created just in case it's not already exist (with the same details)
    and that the existed stop will be modified as needed
    Args:
        stops: a dict that represent the current up-to-date stops
        stop_to_upsert: obj that represent the stop details for upsert

    Returns: Stop
    """

    exist_stop = stops.get(stop_to_upsert.stop_code)
    res_stop: Stop

    # in case stop exist with same values we should return that stop
    if exist_stop is not None and _is_exist_stop_eql_to_stop_to_upsert(exist_stop, stop_to_upsert):
        res_stop = exist_stop

        # in case the stop is in the past, change max date to include current date
        if stop_to_upsert.stop_date > res_stop.max_date:
            res_stop.max_date = stop_to_upsert.stop_date

    # in case stop is not exist, or it is different - create new one
    else:
        res_stop = Stop(min_date=stop_to_upsert.stop_date, max_date=stop_to_upsert.stop_date,
                        code=stop_to_upsert.stop_code, lat=stop_to_upsert.stop_lat, lon=stop_to_upsert.stop_lon,
                        name=stop_to_upsert.stop_name, city=stop_to_upsert.stop_desc_city, is_from_gtfs=True)

        # if it already exists - change the date of the existing stop to yesterday
        if exist_stop is not None:
            exist_stop.max_date = stop_to_upsert.stop_date - timedelta(days=1)

    return res_stop


def _is_exist_stop_eql_to_stop_to_upsert(existing_stop, stop_to_upsert: StopModel):
    return existing_stop.lat == stop_to_upsert.stop_lat and existing_stop.lon == stop_to_upsert.stop_lon \
           and existing_stop.name == stop_to_upsert.stop_name and existing_stop.city == stop_to_upsert.stop_desc_city


def duplicate_route_for_same_date(session: Session, route_from_gtfs):
    same_date: datetime.date = route_from_gtfs.max_date
    route_from_db_from_same_date = session.query(Route). \
        filter(Route.min_date <= same_date,
               same_date <= Route.max_date,
               Route.line_ref == route_from_gtfs.line_ref,
               Route.operator_ref == route_from_gtfs.operator_ref,
               Route.is_from_gtfs == route_from_gtfs.is_from_gtfs).one_or_none()
    return route_from_db_from_same_date is not None


def same_route_exist_yesterday_adjust_dates(session: Session, route_from_gtfs):
    day_before: datetime.date = route_from_gtfs.max_date - timedelta(days=1)

    route_from_db_from_day_before = session.query(Route). \
        filter(Route.min_date <= day_before,
               route_from_gtfs.max_date <= day_before,
               Route.line_ref == route_from_gtfs.line_ref,
               Route.operator_ref == route_from_gtfs.operator_ref,
               Route.is_from_gtfs == route_from_gtfs.is_from_gtfs).one_or_none()

    if route_from_db_from_day_before is None:
        return False

    route_from_db_from_day_before.rides.extend(route_from_gtfs.rides)

    for route_stop in route_from_db_from_day_before.route_stops:
        route_stop.stop.max_date = route_from_gtfs.max_date

    route_from_db_from_day_before.max_date = route_from_gtfs.max_date

    return True


def _get_valid_stops_for_date(session: Session, date_to_analyze: date):
    """
    get last relevant stop version for given date.
    """
    stops = session.query(Stop).filter(Stop.min_date <= date_to_analyze).order_by(Stop.max_date).all()
    # looping over ordered stops and inserting them into dict by stop code will promise that each stop code will have
    # the last relevant version of stop.
    return {stop.code: stop for stop in stops}


@session_decorator
def load_routes_to_db(session: Session, route_stat, date_to_analyze: date):
    stops: Dict[int, Stop] = _get_valid_stops_for_date(session, date_to_analyze)
    for _, route in tqdm(route_stat.iterrows()):
        route_from_gtfs = RouteRecord.from_row(route).convert_into_db_route(stops=stops)

        if not same_route_exist_yesterday_adjust_dates(session, route_from_gtfs) \
                and not duplicate_route_for_same_date(session, route_from_gtfs):
            session.add(route_from_gtfs)

    session.commit()
