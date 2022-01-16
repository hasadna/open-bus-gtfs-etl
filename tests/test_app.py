import datetime
import os
import shutil
from pathlib import Path
from unittest.mock import Mock
from urllib.error import URLError

import numpy
import pytest
import pandas as pd

from open_bus_gtfs_etl.archives import Archives
from open_bus_gtfs_etl.gtfs_extractor.gtfs_extractor import GtfsRetriever, GTFS_EXTRACTOR_CONFIG, GTFSFiles, \
    GTFS_METADATA_FILE, GtfsExtractorConfig, DownloadingException
from open_bus_gtfs_etl.gtfs_stat.gtfs_stats import create_trip_and_route_stat, ROUTE_STAT_FILE_NAME, TRIP_STAT_FILE_NAME
from open_bus_gtfs_etl import analyze, extract
from open_bus_gtfs_etl import config, common


# pylint: disable=unused-argument
def fake_download_file_from_ftp(url, local_file: Path):
    """
    fake method to simulate downloading from ftp. fake data is written without the need for real
    connection to a ftp server
    Args:
        url: not used, needed for the API
        local_file: path to write to.
    """
    with local_file.open('w') as f:
        f.write("foobar")


class TestGtfsExtractor:

    @staticmethod
    def test_retrieve_gtfs_files_with_irrelevant_error_wont_cause_retry(tmp_path: Path):
        """
        Test that in case other error than URLError raise the app wont retry to download GTFS Files.
        """
        # Arrange
        class FooException(Exception):
            pass

        retriever = GtfsRetriever(tmp_path)
        retriever.download_file_from_ftp = Mock(side_effect=FooException("irrelevant error - not suppose to retry"))

        # Act
        with pytest.raises(FooException):
            retriever.retrieve_gtfs_files()

        # Assert
        retriever.download_file_from_ftp.assert_called_once()

    @staticmethod
    def test_retrieve_gtfs_files_with_relevant_error_cause_retry(tmp_path: Path):
        """
        Test that in case URLError raise while trying to download GTFS Files - will cause to retry downloading files
        """
        # Arrange
        gtfs_extractor_config: GtfsExtractorConfig = GTFS_EXTRACTOR_CONFIG.copy()
        gtfs_extractor_config.download_retries_delay = [0, 0, 0, 0]
        number_of_retries = len(gtfs_extractor_config.download_retries_delay) + 1

        retriever = GtfsRetriever(tmp_path, app_config=gtfs_extractor_config)
        retriever.download_file_from_ftp = Mock(side_effect=URLError("irrelevant error - not suppose to retry"))

        # Act
        with pytest.raises(DownloadingException):
            retriever.retrieve_gtfs_files()

        # Assert
        assert retriever.download_file_from_ftp.call_count == number_of_retries

    @staticmethod
    def test_init():
        # Arrange
        folder = Path('foo')

        # Act
        actual = GtfsRetriever(folder=folder)

        # Assert
        assert actual.folder == folder
        assert actual.app_config == GTFS_EXTRACTOR_CONFIG

    @staticmethod
    def test_retrieve_gtfs_files__all_files_created(tmp_path: Path):
        # Arrange
        gtfs_retriever = GtfsRetriever(folder=tmp_path)

        gtfs_retriever.download_file_from_ftp = fake_download_file_from_ftp

        # Act
        actual = gtfs_retriever.retrieve_gtfs_files()

        # Assert
        assert actual.gtfs.is_file()
        assert actual.tariff.is_file()
        assert actual.cluster_to_line.is_file()
        assert actual.trip_id_to_date.is_file()

        assert tmp_path.joinpath(GTFS_METADATA_FILE).is_file()

    @staticmethod
    def test_retrieve_gtfs_files__metadata_file_is_correct(tmp_path: Path):
        # Arrange
        gtfs_retriever = GtfsRetriever(folder=tmp_path)
        gtfs_retriever.download_file_from_ftp = fake_download_file_from_ftp

        # Act
        actual_from_method: GTFSFiles = gtfs_retriever.retrieve_gtfs_files()

        actual_from_file = GTFSFiles.parse_file(tmp_path.joinpath(GTFS_METADATA_FILE))

        assert actual_from_method == actual_from_file


class TestMain:

    @staticmethod
    def test_analyze_gtfs_stat():
        config.GTFS_ETL_ROOT_ARCHIVES_FOLDER = os.path.join('.data', 'tests')
        shutil.rmtree(config.GTFS_ETL_ROOT_ARCHIVES_FOLDER, ignore_errors=True)
        date = datetime.date(2019, 3, 7)
        shutil.copytree('tests/resources/gtfs_extract_assets', common.get_dated_path(date))
        extract.main(date, None)
        analyze.main(date, None)
        route_stats_csv = Path(os.path.join(config.GTFS_ETL_ROOT_ARCHIVES_FOLDER, 'workdir', 'analyzed', 'route_stats.csv'))
        trip_stats_csv = Path(os.path.join(config.GTFS_ETL_ROOT_ARCHIVES_FOLDER, 'workdir', 'analyzed', 'trip_stats.csv'))
        assert route_stats_csv.is_file()
        assert trip_stats_csv.is_file()
        route_stats_df = pd.read_csv(route_stats_csv)
        trip_stats_df = pd.read_csv(trip_stats_csv)
        assert (trip_stats_df.shape, route_stats_df.shape) == ((74, 49), (3, 58))
        route_stats = route_stats_df.to_dict('records')
        trip_stats = trip_stats_df.to_dict('records')
        route_row = route_stats[0]
        assert numpy.isnan(route_row.pop('end_zone'))
        assert numpy.isnan(route_row.pop('start_zone'))
        assert numpy.isnan(route_row.pop('cluster_sub_desc'))
        assert route_row == {
            'agency_id': 25,
            'agency_name': 'אפיקים',
            'all_start_time': '05:10:00;05:40:00;06:00:00;06:20:00;08:45:00;09:10:00;09:45:00;10:10:00;10:45:00;11:05:00;11:25:00;11:45:00;12:25:00;12:45:00;13:25:00;13:45:00;14:45:00;15:45:00;16:45:00;17:50:00;18:50:00;19:10:00;19:50:00;20:35:00;21:05:00;21:35:00',
            'all_stop_code': '37471;31272;31273;31512;31513;31556;31514;31515;31230;31231;32217;31516;32043;32208;32044;31517;31518;31520;31521',
            'all_stop_desc_city': 'יבנה;יבנה;יבנה;יבנה;יבנה;יבנה;יבנה;יבנה;יבנה;יבנה;יבנה;יבנה;יבנה;יבנה;יבנה;יבנה;יבנה;יבנה;יבנה',
            'all_stop_id': '38725;15582;15583;15736;15737;15778;15738;15739;15542;15543;16155;15740;16019;16148;16020;15741;15742;15744;15745',
            'all_stop_latlon': '31.8907,34.731190999999995;31.887865,34.734514000000004;31.885345,34.738877;31.881099,34.741085;31.87862,34.742967;31.875793,34.741625;31.874412,34.740362;31.873075,34.739788;31.87139,34.742241;31.874171,34.743827;31.876914000000003,34.74539;31.877638,34.746447;31.875752,34.74938;31.871982,34.748531;31.869305,34.746559000000005;31.869489,34.74453;31.868694,34.742726;31.864023,34.742128;31.862123,34.744215000000004',
            'all_stop_name': 'ת. רכבת יבנה מערב;שד. הסנהדרין/שד. ירושלים;שד. '
                             'הסנהדרין/הירדן;האלון/האשל;האלון/תיכון גינסבורג;האלון/אבו '
                             'חצירא;האלון/דואני;שד. דואני/סחלב;שד. דואני/הדרור;הדרור/אבו '
                             "חצירא;הדרור/דוכיפת;אהרון חג'ג'/הדרור;'שדרות העצמאות/אהרון "
                             "חג'ג;העצמאות/אבו חצירא;העצמאות/דואני;עירייה/שד. "
                             'דואני;גיבורי החיל/גיבורי ישראל;המיסב/העמל;ת. רכבת יבנה מזרח',
            'all_trip_id': '30900053_060319;30900054_060319;30900055_060319;30900056_060319;30900057_060319;30900058_060319;30900059_060319;30900060_060319;30900061_060319;30900062_060319;30900063_060319;30900064_060319;30900065_060319;30900066_060319;30900067_060319;30900068_060319;30900069_060319;30900070_060319;30900071_060319;30900072_060319;30900073_060319;30900074_060319;30900075_060319;30900076_060319;30900077_060319;30900078_060319',
            'all_trip_id_to_date': '30900157;30900158;30900159;30900160;30900161;30900162;30900163;30900164;30900165;30900166;30900167;30900168;30900169;30900170;30900171;30900172;30900173;30900174;30900175;30900176;30900177;30900178;30900179;30900180;30900181;30900182',
            'cluster_id': 158,
            'cluster_name': 'אשדוד-יבנה-ת"א',
            'date': '2019-03-07',
            'end_stop_city': 'יבנה',
            'end_stop_desc': 'רחוב:  עיר: יבנה רציף:   קומה:',
            'end_stop_id': 15745,
            'end_stop_lat': 31.862123,
            'end_stop_lon': 34.744215000000004,
            'end_stop_name': 'ת. רכבת יבנה מזרח',
            'end_time': '21:54:38',
            'is_bidirectional': 0,
            'is_loop': 0,
            'line_type': 1,
            'line_type_desc': 'עירוני',
            'max_headway': 65.0,
            'mean_headway': 37.8125,
            'mean_trip_distance': 7044.0,
            'mean_trip_duration': 0.3272222222222222,
            'min_headway': 20.0,
            'num_stops': 19,
            'num_trip_ends': 26,
            'num_trip_starts': 26,
            'num_trips': 26,
            'num_zones': 0,
            'num_zones_missing': 19,
            'peak_end_time': '05:29:38',
            'peak_num_trips': 1,
            'peak_start_time': '05:10:00',
            'route_alternative': '#',
            'route_direction': 1,
            'route_id': 1,
            'route_long_name': 'ת. רכבת יבנה מערב-יבנה<->ת. רכבת יבנה מזרח-יבנה-1#',
            'route_mkt': 67001,
            'route_short_name': 1,
            'route_type': 3,
            'service_distance': 183144,
            'service_duration': 8.507777777777777,
            'service_speed': 21.52665534804754,
            'start_stop_city': 'יבנה',
            'start_stop_desc': 'רחוב:  עיר: יבנה רציף:   קומה:',
            'start_stop_id': 38725,
            'start_stop_lat': 31.8907,
            'start_stop_lon': 34.731190999999995,
            'start_stop_name': 'ת. רכבת יבנה מערב',
            'start_time': '05:10:00',
        }
        trip_row = trip_stats[0]
        assert numpy.isnan(trip_row.pop('end_zone'))
        assert numpy.isnan(trip_row.pop('start_zone'))
        assert numpy.isnan(trip_row.pop('cluster_sub_desc'))
        assert trip_row == {
            'agency_id': 25,
            'agency_name': 'אפיקים',
            'all_stop_code': '31521;32500;31759;32502;31531;31532;31233;35212;32504;32505;32506;31278;32131;32132;32507;32508;32509;32510;32511;32512;32219;32220;32221;37471',
            'all_stop_desc_city': 'יבנה;יבנה;יבנה;יבנה;יבנה;יבנה;יבנה;יבנה;יבנה;יבנה;יבנה;יבנה;יבנה;יבנה;יבנה;יבנה;יבנה;יבנה;יבנה;יבנה;יבנה;יבנה;יבנה;יבנה',
            'all_stop_id': '15745;16424;41485;16426;15754;15755;15545;23210;16428;16429;16430;15588;16077;16078;16431;16432;16433;16434;16435;16436;16157;16158;16159;38725',
            'all_stop_latlon': '31.862123,34.744215000000004;31.864043,34.742254;31.865428,34.74128;31.868769,34.743021;31.871096,34.744669;31.873145,34.745963;31.873257,34.747204;31.873051,34.749224;31.874993,34.749429;31.876659000000004,34.748984;31.877721,34.74649;31.876841,34.745235;31.873909,34.743564;31.871494,34.742185;31.871369,34.74148;31.873086,34.740038;31.874007,34.740077;31.876149,34.742065999999994;31.879317,34.742811;31.881094,34.741227;31.88331,34.741063;31.885968,34.738341;31.888449,34.733818;31.8907,34.731190999999995',
            'all_stop_name': 'ת. רכבת יבנה מזרח;המיסב/העצמאות;גיבורי החייל/המיסב;גיבורי '                  'החיל/התנאים;שבזי/בוכריס;שבזי/שד. הגרי זכריה;אבו '                  "חצירא/חבלבל;העצמאות/הנשיאים;שדרות העצמאות;'שד. "                  "העצמאות/אהרון חג'ג;אהרון "                  "חג'ג'/הצבעוני;הדרור/דוכיפת;הדרור/הזמיר;הדרור/הנשר;שד. "                  'דואני/קדושי קהיר;שדרות דואני/האלון;האלון/שדרות '                  'דואני;האלון/אבו חצירא;האלון/האורן;האלון/האשל;שד. '                  "הסנהדרין/שד. ז'בוטינסקי;שד. הסנהדרין/הירדן;שד. הסנהדרין/שד. "                  'ירושלים;ת. רכבת יבנה מערב',
            'cluster_id': 158,
            'cluster_name': 'אשדוד-יבנה-ת"א',
            'date': '2019-03-07',
            'direction_id': 1,
            'distance': 6884,
            'duration': 0.3036111111111111,
            'end_stop_city': 'יבנה',
            'end_stop_code': 37471,
            'end_stop_desc': 'רחוב:  עיר: יבנה רציף:   קומה:',
            'end_stop_id': 38725,
            'end_stop_lat': 31.8907,
            'end_stop_lon': 34.731190999999995,
            'end_stop_name': 'ת. רכבת יבנה מערב',
            'end_time': '06:43:13',
            'is_loop': 0,
            'line_type': 1,
            'line_type_desc': 'עירוני',
            'num_stops': 24,
            'num_zones': 0,
            'num_zones_missing': 24,
            'route_alternative': '#',
            'route_direction': 2,
            'route_id': 2,
            'route_long_name': 'ת. רכבת יבנה מזרח-יבנה<->ת. רכבת יבנה מערב-יבנה-2#',
            'route_mkt': 67001,
            'route_short_name': 1,
            'route_type': 3,
            'shape_id': 103446,
            'speed': 22.67374199451052,
            'start_stop_city': 'יבנה',
            'start_stop_code': 31521,
            'start_stop_desc': 'רחוב:  עיר: יבנה רציף:   קומה:',
            'start_stop_id': 15745,
            'start_stop_lat': 31.862123,
            'start_stop_lon': 34.744215000000004,
            'start_stop_name': 'ת. רכבת יבנה מזרח',
            'start_time': '06:25:00',
            'trip_id': '25227535_060319',
            'trip_id_to_date': 25227639
        }


class TestArchives:
    @staticmethod
    def test_get_dated_path():
        path = Path('tests/resources/example_archive_folder')
        actual = Archives(path).gtfs.get_dated_path(datetime.date(2000, 10, 20))
        expected = Archives(path).gtfs.root_folder.joinpath('2000', '10', '20')
        assert expected == actual

    @staticmethod
    def test_get_dated_path_with_file():
        path = Path('tests/resources/example_archive_folder')
        actual = Archives(path).gtfs.get_dated_path(datetime.date(2000, 10, 20), 'filename')
        expected = Archives(path).gtfs.root_folder.joinpath('2000', '10', '20', 'filename')
        assert expected == actual
