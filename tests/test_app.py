import datetime
import os
from pathlib import Path
from typing import Dict
from unittest.mock import Mock
from urllib.error import URLError

import pytest
from open_bus_stride_db.model import Stop

from open_bus_gtfs_etl.archives import Archives
from open_bus_gtfs_etl.gtfs_extractor.gtfs_extractor import GtfsRetriever, GTFS_EXTRACTOR_CONFIG, GTFSFiles, \
    GTFS_METADATA_FILE, GtfsExtractorConfig, DownloadingException
from open_bus_gtfs_etl.gtfs_loader import _upsert_stop, StopModel
from open_bus_gtfs_etl.gtfs_stat.gtfs_stats import create_trip_and_route_stat, ROUTE_STAT_FILE_NAME, TRIP_STAT_FILE_NAME
from open_bus_gtfs_etl.api import write_gtfs_metadata_into_file, analyze_gtfs_stat, \
    analyze_gtfs_stat_into_archive_folder, download_gtfs_files_into_archive_folder


# pylint: disable=unused-argument
def fake_download_file_from_ftp(url, local_file: Path):
    with local_file.open('w') as f:
        f.write("foobar")


class TestGtfsExtractor:

    def test_retrieve_gtfs_files_with_irrelevant_error_wont_cause_retry(self, tmp_path: Path):
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

    def test_retrieve_gtfs_files_with_relevant_error_cause_retry(self, tmp_path: Path):
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

    def test_init(self):
        # Arrange
        folder = Path('foo')

        # Act
        actual = GtfsRetriever(folder=folder)

        # Assert
        assert actual.folder == folder
        assert actual.app_config == GTFS_EXTRACTOR_CONFIG

    def test_retrieve_gtfs_files__all_files_created(self, tmp_path: Path):
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

    def test_retrieve_gtfs_files__metadata_file_is_correct(self, tmp_path: Path):
        # Arrange
        gtfs_retriever = GtfsRetriever(folder=tmp_path)
        gtfs_retriever.download_file_from_ftp = fake_download_file_from_ftp

        # Act
        actual_from_method: GTFSFiles = gtfs_retriever.retrieve_gtfs_files()

        actual_from_file = GTFSFiles.parse_file(tmp_path.joinpath(GTFS_METADATA_FILE))

        assert actual_from_method == actual_from_file


class TestMain:
    def test_write_gtfs_metadata_into_file(self, tmp_path: Path):
        # Arrange
        base = Path(__file__).parent.joinpath('resources', 'gtfs_extract_assets')
        output = tmp_path.joinpath('metadata.file')

        # Act
        write_gtfs_metadata_into_file(gtfs=base.joinpath("2019-03-07-israel-public-transportation.zip"),
                                      tariff=base.joinpath("2019-03-07-Tariff.zip"),
                                      cluster_to_line=base.joinpath("2019-03-07-ClusterToLine.zip"),
                                      trip_id_to_date=base.joinpath("2019-03-07-TripIdToDate.zip"),
                                      output=output)

        # Assert
        assert GTFSFiles.parse_file(output)

    @pytest.mark.skip('missing test data to run this test')
    def test_analyze_gtfs_stat_into_archive_folder(self):



        analyze_gtfs_stat_into_archive_folder(datetime.date(2019, 3, 7), Archives(Path(__file__).parent.joinpath('resources/example_archive_folder')))


        """"
        resources/example_archive_folder/gtfs_archive/2019/03/07/.gtfs_metadata.json
        resources/example_archive_folder/gtfs_archive/2019/03/07/.gtfs_metadata.json
        """

    def test_analyze_gtfs_stat(self, tmp_path):

        trip_stats, route_stats = analyze_gtfs_stat(date_to_analyze=datetime.date(2019, 3, 7),
                                                    output_folder=tmp_path,
                                                    gtfs_metadata_file=Path('tests/resources/gtfs_extract_assets/.gtfs_metadata.json'))

        assert tmp_path.joinpath(ROUTE_STAT_FILE_NAME).is_file()
        assert tmp_path.joinpath(TRIP_STAT_FILE_NAME).is_file()
        assert (trip_stats.shape, route_stats.shape) == ((74, 49), (3, 58))

    def test_analyze_gtfs_stat_with_output_folder_with_a_dot_in_name(self, tmp_path):
        """
        Test a use case described in https://github.com/hasadna/open-bus/issues/345
        """

        tmp_path = tmp_path.joinpath('.data')
        os.mkdir(tmp_path)

        trip_stats, route_stats = analyze_gtfs_stat(date_to_analyze=datetime.date(2019, 3, 7),
                                                    output_folder=tmp_path,
                                                    gtfs_metadata_file=Path('./tests/resources/gtfs_extract_assets/'
                                                                            '.gtfs_metadata.json'))

        assert tmp_path.joinpath(ROUTE_STAT_FILE_NAME).is_file()
        assert tmp_path.joinpath(TRIP_STAT_FILE_NAME).is_file()
        assert (trip_stats.shape, route_stats.shape) == ((74, 49), (3, 58))


class TestGtgsStat:
    def test_create_trip_and_route_stat(self, tmp_path: Path):
        # Arrange
        base = Path(__file__).parent.joinpath('resources', 'gtfs_extract_assets')
        gtfs_files = GTFSFiles.parse_file(Path(base).joinpath(GTFS_METADATA_FILE))

        # Act
        trip_stat, route_stat = create_trip_and_route_stat(date_to_analyze=datetime.date(2019, 3, 7),
                                                           gtfs_files=gtfs_files)
        # Assert

        assert trip_stat.shape == (74, 49)
        assert route_stat.shape == (3, 58)


class TestArchives:
    def test_get_dated_path(self):
        path = Path('tests/resources/example_archive_folder')
        actual = Archives(path).gtfs.get_dated_path(datetime.date(2000, 10, 20))
        expected = Archives(path).gtfs.root_folder.joinpath('2000', '10', '20')
        assert expected == actual

    def test_get_dated_path_with_file(self):
        path = Path('tests/resources/example_archive_folder')
        actual = Archives(path).gtfs.get_dated_path(datetime.date(2000, 10, 20), 'filename')
        expected = Archives(path).gtfs.root_folder.joinpath('2000', '10', '20', 'filename')
        assert expected == actual


class TestUpsertStop:
    def test_upsert_stop_if_not_exist_stop_create_new_one(self):
        today = datetime.date(2020, 10, 15)

        stop_to_upsert = StopModel(stop_code=555, stop_lat=15.7, stop_lon=13.4, stop_name="aba-hillel",
                                   stop_desc_city="ramat-gan", stop_date=today)

        actual = _upsert_stop({}, stop_to_upsert)

        assert actual.max_date == today and actual.min_date == today

    def test_upsert_stop_if_exist_stop_but_different_create_new_and_update_existing(self):
        today = datetime.date(2020, 10, 15)
        a_week_ago = today - datetime.timedelta(days=7)
        yesterday = today - datetime.timedelta(days=1)

        exist_stop_a_week_ago = Stop(min_date=a_week_ago, max_date=a_week_ago, code=555, lat=99, lon=99,
                                     name="", city="", is_from_gtfs=True)

        stop_to_upsert = StopModel(stop_code=555, stop_lat=15.7, stop_lon=13.4, stop_name="aba-hillel",
                                   stop_desc_city="ramat-gan", stop_date=today)

        actual = _upsert_stop({exist_stop_a_week_ago.code: exist_stop_a_week_ago}, stop_to_upsert)

        assert actual.max_date == today and actual.min_date == today
        assert exist_stop_a_week_ago.max_date == yesterday

    def test_upsert_stop_if_exist_stop_and_same_update_existing(self):
        today = datetime.date(2020, 10, 15)
        a_week_ago = today - datetime.timedelta(days=7)

        exist_stop_a_week_ago = Stop(min_date=a_week_ago, max_date=a_week_ago, code=555, lat=15.7, lon=13.4,
                                     name="aba-hillel", city="ramat-gan", is_from_gtfs=True)

        stop_to_upsert = StopModel(stop_code=555, stop_lat=15.7, stop_lon=13.4, stop_name="aba-hillel",
                                   stop_desc_city="ramat-gan", stop_date=today)

        actual = _upsert_stop({exist_stop_a_week_ago.code: exist_stop_a_week_ago}, stop_to_upsert)

        assert actual == exist_stop_a_week_ago
        assert exist_stop_a_week_ago.max_date == today

    def test_upsert_stop_if_exist_stop_for_same_dates_update_existing_without_creating_new(self):
        today = datetime.date(2020, 10, 15)
        a_week_ago = today - datetime.timedelta(days=7)

        exist_stop_for_today = Stop(min_date=today, max_date=today, code=555, lat=1, lon=1,
                                    name="", city="", is_from_gtfs=True)

        stop_to_upsert = StopModel(stop_code=555, stop_lat=15.7, stop_lon=13.4, stop_name="aba-hillel",
                                   stop_desc_city="ramat-gan", stop_date=today)

        actual = _upsert_stop({exist_stop_for_today.code: exist_stop_for_today}, stop_to_upsert)

        assert actual == exist_stop_for_today
        assert actual.max_date == today
        assert actual.min_date == today
        assert actual.lat == stop_to_upsert.stop_lat
        assert actual.lon == stop_to_upsert.stop_lon
        assert actual.name == stop_to_upsert.stop_name
        assert actual.city == stop_to_upsert.stop_desc_city
