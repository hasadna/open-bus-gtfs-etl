import unittest
import datetime
from pathlib import Path

from open_bus_stride_db import db
from open_bus_stride_db.model import GtfsRoute
from sqlalchemy.orm import Session
from open_bus_gtfs_etl.gtfs_loader import RouteStatRecord, Loader, StopStatRecord, RideStatRecord

route_for_example = RouteStatRecord(date=datetime.date(2021, 12, 11), route_id=1, agency_id=25, route_short_name='1',
                                    route_long_name='ת. רכבת יבנה מערב-יבנה<->ת. רכבת יבנה מזרח-יבנה-1#',
                                    route_mkt=67001, route_direction=1, route_alternative='#',
                                    agency_name='אלקטרה אפיקים', route_type=3, rides=[
        RideStatRecord(trip_id_to_date=16681576, trip_id=16681576111221,
                       start_time=datetime.datetime(2021, 12, 11, 20, 20,
                                                    tzinfo=datetime.timezone(datetime.timedelta(seconds=7200)))),
        RideStatRecord(trip_id_to_date=18547574, trip_id=18547574111221,
                       start_time=datetime.datetime(2021, 12, 11, 19, 19,
                                                    tzinfo=datetime.timezone(datetime.timedelta(seconds=7200)))),
        RideStatRecord(trip_id_to_date=18547575, trip_id=18547575111221,
                       start_time=datetime.datetime(2021, 12, 11, 20, 20,
                                                    tzinfo=datetime.timezone(datetime.timedelta(seconds=7200)))),
        RideStatRecord(trip_id_to_date=18921397, trip_id=18921397111221,
                       start_time=datetime.datetime(2021, 12, 11, 19, 19,
                                                    tzinfo=datetime.timezone(datetime.timedelta(seconds=7200)))),
        RideStatRecord(trip_id_to_date=20085099, trip_id=20085099111221,
                       start_time=datetime.datetime(2021, 12, 11, 18, 18,
                                                    tzinfo=datetime.timezone(datetime.timedelta(seconds=7200)))),
        RideStatRecord(trip_id_to_date=84, trip_id=84111221, start_time=datetime.datetime(2021, 12, 11, 21, 21,
                                                                                          tzinfo=datetime.timezone(
                                                                                              datetime.timedelta(
                                                                                                  seconds=7200)))),
        RideStatRecord(trip_id_to_date=85, trip_id=85111221, start_time=datetime.datetime(2021, 12, 11, 22, 22,
                                                                                          tzinfo=datetime.timezone(
                                                                                              datetime.timedelta(
                                                                                                  seconds=7200)))),
        RideStatRecord(trip_id_to_date=86, trip_id=86111221, start_time=datetime.datetime(2021, 12, 11, 22, 22,
                                                                                          tzinfo=datetime.timezone(
                                                                                              datetime.timedelta(
                                                                                                  seconds=7200)))),
        RideStatRecord(trip_id_to_date=87, trip_id=87111221, start_time=datetime.datetime(2021, 12, 11, 23, 23,
                                                                                          tzinfo=datetime.timezone(
                                                                                              datetime.timedelta(
                                                                                                  seconds=7200))))],
                                    stops=[
                                        StopStatRecord(desc_city='יבנה', stop_name='ת. רכבת יבנה מערב', stop_id=38725,
                                                       stop_code=37471, lat=31.8907, lon=34.731191),
                                        StopStatRecord(desc_city='יבנה', stop_name='שד. הסנהדרין/שד. ירושלים',
                                                       stop_id=15582, stop_code=31272, lat=31.887865, lon=34.734514),
                                        StopStatRecord(desc_city='יבנה', stop_name='שד. הסנהדרין/הירדן', stop_id=15583,
                                                       stop_code=31273, lat=31.885345, lon=34.738877),
                                        StopStatRecord(desc_city='יבנה', stop_name='האלון/האשל', stop_id=15736,
                                                       stop_code=31512, lat=31.881099, lon=34.741085),
                                        StopStatRecord(desc_city='יבנה', stop_name='האלון/תיכון גינסבורג',
                                                       stop_id=15737, stop_code=31513, lat=31.87862, lon=34.742967),
                                        StopStatRecord(desc_city='יבנה', stop_name='האלון/אבו חצירא', stop_id=15778,
                                                       stop_code=31556, lat=31.875793, lon=34.741625),
                                        StopStatRecord(desc_city='יבנה', stop_name='האלון/דואני', stop_id=15738,
                                                       stop_code=31514, lat=31.874412, lon=34.740362),
                                        StopStatRecord(desc_city='יבנה', stop_name='שד. דואני/סחלב', stop_id=15739,
                                                       stop_code=31515, lat=31.873071, lon=34.739786),
                                        StopStatRecord(desc_city='יבנה', stop_name='הדרור/שדרות דואני', stop_id=15542,
                                                       stop_code=31230, lat=31.87139, lon=34.742241),
                                        StopStatRecord(desc_city='יבנה', stop_name='הדרור/אבו חצירא', stop_id=15543,
                                                       stop_code=31231, lat=31.874171, lon=34.743827),
                                        StopStatRecord(desc_city='יבנה', stop_name='הדרור/דוכיפת', stop_id=16155,
                                                       stop_code=32217, lat=31.876914, lon=34.74539),
                                        StopStatRecord(desc_city='יבנה', stop_name="אהרון חג'ג'/הדרור", stop_id=15740,
                                                       stop_code=31516, lat=31.877638, lon=34.746447),
                                        StopStatRecord(desc_city='יבנה', stop_name="שדרות העצמאות/אהרון חג'ג'",
                                                       stop_id=16019, stop_code=32043, lat=31.875752, lon=34.74938),
                                        StopStatRecord(desc_city='יבנה', stop_name='העצמאות/אבו חצירא', stop_id=16148,
                                                       stop_code=32208, lat=31.871982, lon=34.748531),
                                        StopStatRecord(desc_city='יבנה', stop_name='העצמאות/דואני', stop_id=16020,
                                                       stop_code=32044, lat=31.869305, lon=34.746559),
                                        StopStatRecord(desc_city='יבנה', stop_name='עירייה/שד. דואני', stop_id=15741,
                                                       stop_code=31517, lat=31.869487, lon=34.744526),
                                        StopStatRecord(desc_city='יבנה', stop_name='גיבורי החיל/עולי הגרדום',
                                                       stop_id=15742, stop_code=31518, lat=31.868694, lon=34.742726),
                                        StopStatRecord(desc_city='יבנה', stop_name='המיסב/העמל', stop_id=15744,
                                                       stop_code=31520, lat=31.864023, lon=34.742128),
                                        StopStatRecord(desc_city='יבנה', stop_name='ת. רכבת יבנה מזרח', stop_id=15745,
                                                       stop_code=31521, lat=31.862017, lon=34.744082)])


class TestLoader(unittest.TestCase):
    def test_get_routes_from_stat_file(self):
        # Arrange
        expected = route_for_example

        # Act
        routes_iter = Loader(Path('tests/resources/simple_route_stat.csv')).get_routes_from_stat_file()
        actual = next(routes_iter)

        # Assert
        self.assertEqual(expected, actual)

    @staticmethod
    def test_evaluate_upsert_action():
        """
        trying to add same route twice will end up with one record only

        """
        # Arrange
        engine = db.engine  # create_engine('sqlite://')
        # meta.create_all(engine)

        # Act
        for _ in range(2):
            Loader(Path('tests/resources/simple_route_stat.csv')).upsert_routes(engine)

        # Assert (That no exception will be raised when checking that there are only single legit record of route)
        with Session(engine) as session:
            session.query(GtfsRoute). \
                filter(GtfsRoute.route_mkt == str(route_for_example.route_mkt),
                       GtfsRoute.date == route_for_example.date,
                       GtfsRoute.route_direction == str(route_for_example.route_direction),
                       GtfsRoute.route_alternative == route_for_example.route_alternative).one()


if __name__ == '__main__':
    unittest.main()
