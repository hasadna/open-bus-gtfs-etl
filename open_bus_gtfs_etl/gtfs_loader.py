import datetime
from datetime import date
from pathlib import Path
from typing import Optional, List, Any, Dict, Generator

from open_bus_stride_db import db
from open_bus_stride_db.model import GtfsRoute, GtfsRouteStop, GtfsStop, GtfsRide
from pandas import DataFrame
from pydantic import BaseModel
from sqlalchemy.exc import MultipleResultsFound, SQLAlchemyError
from sqlalchemy.orm import Session
from tqdm import tqdm

from open_bus_gtfs_etl.gtfs_stat.output import read_stat_file


class UnsupportedDatabaseState(Exception):
    """
    Exception that represent unsupported state of database
    """


class DatabaseException(Exception):
    """
    Exception that represent an error related to database
    """


class RideStatRecord(BaseModel):
    """
    represent the ride section in route stat record
    """
    trip_id_to_date: int
    trip_id: int
    start_time: datetime.datetime

    def convert_to_gtfs_ride(self) -> GtfsRide:
        """
        convert RideStatRecord model instance into database record

        following https://www.gov.il/BlobFolder/generalpage/real_time_information_siri/he/ICD_SM_28_31.pdf
        "journey_ref" should be TripId at TripIdToDate.txt.

        Returns: GtfsRide

        """
        return GtfsRide(scheduled_start_time=self.start_time, journey_ref=self.trip_id_to_date)


class StopStatRecord(BaseModel):
    """
    represent the stop section in route stat record
    """
    desc_city: str
    stop_name: str
    stop_id: int
    stop_code: int
    lat: float
    lon: float

    def convert_to_gtfs_stop(self, update_date: date) -> GtfsStop:
        """
        convertor method into GtfsStop object that represent a database record
        Args:
            update_date:

        Returns: GtfsStop

        """
        return GtfsStop(date=update_date, code=self.stop_code, lat=self.lat, lon=self.lon, name=self.stop_name,
                        city=self.desc_city)


def handel_special_time_format(base_date: str, special_time_format: str) -> datetime.datetime:
    """
    MOT uses special time format to represent a time for a ride that occurs after midnight.
    for example, in case a ride is planned for 2AM we will get time like: 26:00:00 which is grater than the convention
    that there are 24 hours in a day. so in case the date is 2022-05-01 and the time of the departure is 26:00:00, we
    should translate it into the right datetime: 2022-05-02T02:00:00
    Args:
        base_date: iso style date string like: 2020-03-23
        special_time_format: time that could be more than 24 hours like 26:45:00

    Returns: combined datetime
    """
    base_datetime = datetime.datetime.fromisoformat(base_date)
    time_parts = special_time_format.split(":")
    time_delta = datetime.timedelta(hours=int(time_parts[0].strip()), minutes=int(time_parts[0].strip()))

    return (base_datetime + time_delta).replace(tzinfo=datetime.timezone(datetime.timedelta(hours=2)))


class RouteStatRecord(BaseModel):
    """
    represent route stat record
    """
    date: datetime.date
    route_id: int
    agency_id: int
    route_short_name: str
    route_long_name: str
    route_mkt: int
    route_direction: int
    route_alternative: str
    agency_name: str
    route_type: int
    rides: List[RideStatRecord]
    stops: List[StopStatRecord]

    @classmethod
    def from_row(cls, args: Dict[str, Any]) -> "RouteStatRecord":
        """
        parse stat row into RouteStatRecord the includes list of StopStatRecord
        Args:
            args: stat row as a dict
        Returns: RouteStatRecord
        """
        args['stops'] = cls._extract_stops(args)
        args['rides'] = cls._extract_rides(args)
        return cls(**args)

    @classmethod
    def _extract_stops(cls, args) -> List[StopStatRecord]:
        stops = []
        all_stop_lat_lon = [tuple(float(i) for i in pair.split(','))
                            for pair in args.pop('all_stop_latlon').split(';')]
        all_stop_desc_city = list(args.pop('all_stop_desc_city').split(';'))
        all_stop_name = args.pop('all_stop_name').split(';')
        all_stop_id = (int(i) for i in args.pop('all_stop_id').split(';'))
        all_stop_code = (int(i) for i in args.pop('all_stop_code').split(';'))
        for stop_lat_lon, stop_desc_city, stop_name, stop_id, stop_code \
                in zip(all_stop_lat_lon, all_stop_desc_city, all_stop_name, all_stop_id, all_stop_code):
            lat, lon = stop_lat_lon
            stops.append(StopStatRecord(desc_city=stop_desc_city, stop_name=stop_name, stop_id=stop_id,
                                        stop_code=stop_code, lat=lat, lon=lon))
        return stops

    @classmethod
    def _extract_rides(cls, args) -> List[RideStatRecord]:

        all_trip_id_to_date = [int(i) for i in args['all_trip_id_to_date'].split(';')]
        all_trip_id = list(args['all_trip_id'].split(';'))
        all_start_time = list(args['all_start_time'].split(';'))

        res = []
        for trip_id_to_date, trip_id, start_time in zip(all_trip_id_to_date, all_trip_id, all_start_time):

            res.append(RideStatRecord(trip_id_to_date=trip_id_to_date, trip_id=trip_id,
                                      start_time=handel_special_time_format(args['date'], start_time)))
        return res

    def convert_to_gtfs_route_record(self) -> GtfsRoute:
        """
                convert RideStatRecord model instance into database record
        Returns:

        """
        return GtfsRoute(date=self.date, line_ref=self.route_id, operator_ref=self.agency_id,
                         route_short_name=self.route_short_name, route_long_name=self.route_long_name,
                         route_mkt=self.route_mkt, route_direction=self.route_direction,
                         route_alternative=self.route_alternative, agency_name=self.agency_name,
                         route_type=self.route_type,
                         gtfs_route_stops=[GtfsRouteStop(order=k,
                                                         gtfs_stop=v.convert_to_gtfs_stop(update_date=self.date))
                                           for k, v in enumerate(self.stops, start=1)],
                         gtfs_rides=[i.convert_to_gtfs_ride() for i in self.rides])


class Loader:
    """
    class that manages the ETL from stat file into DB
    """

    def __init__(self, route_stat_path: Path, date_to_load: datetime.date = None):
        self.route_stat_path = route_stat_path
        if date_to_load is None:
            date_to_load = datetime.date.today()
        self.date_to_load = date_to_load

    def get_routes_from_stat_file(self) -> Generator[RouteStatRecord, None, None]:
        """
        Parse Route Stat file into iterator of RouteRecord

        """
        route_stat_content: DataFrame = read_stat_file(path=self.route_stat_path)
        route_iter = route_stat_content.iterrows()
        return (RouteStatRecord.from_row(dict(stat_file_row.iteritems()))
                for _, stat_file_row
                in tqdm(total=len(route_stat_content.index), iterable=route_iter,
                        desc='Processing Routes From Stat File'))

    def upsert_routes(self, engine=None):
        """
        upsert operation that in case same route is found will update it (delete the old one and create a new record),
        in case there is no same route - a new route will be created.
        UnsupportedDatabaseState exception will be raised in case there is no unique route record per day in database
        DatabaseException exception will be raised in case there is error related to database
        """
        if engine is None:
            engine = db.engine

        with Session(engine) as session:
            try:
                for route in self.get_routes_from_stat_file():
                    self._delete_route_if_exists(session, route)

                    session.add(route.convert_to_gtfs_route_record())
                session.commit()
            except SQLAlchemyError as err:
                raise DatabaseException("got unexpected error from DB") from err

    @staticmethod
    def _delete_route_if_exists(session: Session, route: RouteStatRecord):
        """
        in case a record of a route is already existed for that date - delete it.
        MOT Documentation says that the combination of route mkt, direction, and alternative is unique for given date
        in case there are more than one record with that combination, UnsupportedDatabaseState exception will be raised
        Args:
            session:
            route:
        """
        try:
            existed_record = session.query(GtfsRoute). \
                filter(GtfsRoute.route_mkt == str(route.route_mkt), GtfsRoute.date == route.date,
                       GtfsRoute.route_direction == str(route.route_direction),
                       GtfsRoute.route_alternative == route.route_alternative).one_or_none()
        except MultipleResultsFound as err:
            raise UnsupportedDatabaseState('expected that the combination of route mkt, date, direction, and '
                                           'alternative will be unique but it is not') from err
        if existed_record:
            session.delete(existed_record)
