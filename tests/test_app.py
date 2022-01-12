import datetime
import os
from pathlib import Path
from unittest.mock import Mock
from urllib.error import URLError

import pytest

from open_bus_gtfs_etl.archives import Archives
from open_bus_gtfs_etl.gtfs_extractor.gtfs_extractor import GtfsRetriever, GTFS_EXTRACTOR_CONFIG, GTFSFiles, \
    GTFS_METADATA_FILE, GtfsExtractorConfig, DownloadingException
from open_bus_gtfs_etl.gtfs_stat.gtfs_stats import create_trip_and_route_stat, ROUTE_STAT_FILE_NAME, TRIP_STAT_FILE_NAME
from open_bus_gtfs_etl.api import write_gtfs_metadata_into_file, analyze_gtfs_stat


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

    def test_analyze_gtfs_stat(self, tmp_path):

        trip_stats, route_stats, _ = analyze_gtfs_stat(date_to_analyze=datetime.date(2019, 3, 7),
                                                       output_folder=tmp_path,
                                                       gtfs_metadata_file=Path(
                                                           'tests/resources/gtfs_extract_assets/.gtfs_metadata.json'))

        assert tmp_path.joinpath(ROUTE_STAT_FILE_NAME).is_file()
        assert tmp_path.joinpath(TRIP_STAT_FILE_NAME).is_file()
        assert (trip_stats.shape, route_stats.shape) == ((74, 49), (3, 58))

    @staticmethod
    def test_analyze_gtfs_stat_with_output_folder_with_a_dot_in_name(tmp_path):
        """
        Test a use case described in https://github.com/hasadna/open-bus/issues/345
        """

        tmp_path = tmp_path.joinpath('.data')
        os.mkdir(tmp_path)

        trip_stats, route_stats, route_stat_file = analyze_gtfs_stat(date_to_analyze=datetime.date(2019, 3, 7),
                                                    output_folder=tmp_path,
                                                    gtfs_metadata_file=Path('./tests/resources/gtfs_extract_assets/'
                                                                            '.gtfs_metadata.json'))
        assert tmp_path.joinpath(route_stat_file).is_file()
        assert tmp_path.joinpath(ROUTE_STAT_FILE_NAME).is_file()
        assert tmp_path.joinpath(TRIP_STAT_FILE_NAME).is_file()
        assert (trip_stats.shape, route_stats.shape) == ((74, 49), (3, 58))


class TestGtgsStat:

    @staticmethod
    def test_create_trip_and_route_stat(tmp_path: Path):
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
