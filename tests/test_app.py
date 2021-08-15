import datetime
import os
from pathlib import Path

from open_bus_gtfs_etl.gtfs_extractor.gtfs_extractor import GtfsRetriever, GTFS_EXTRACTOR_CONFIG, GTFSFiles, \
    GTFS_METADATA_FILE
from open_bus_gtfs_etl.gtfs_stat.gtfs_stats import create_trip_and_route_stat, ROUTE_STAT_FILE_NAME, TRIP_STAT_FILE_NAME
from open_bus_gtfs_etl.api import write_gtfs_metadata_into_file, analyze_gtfs_stat, get_archive_folder_path


# pylint: disable=unused-argument
def fake_download_file_from_ftp(url, local_file: Path):
    with local_file.open('w') as f:
        f.write("foobar")


class TestGtfsExtractor:
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

        trip_stats, route_stats = analyze_gtfs_stat(date_to_analyze=datetime.datetime(2019, 3, 7),
                                                    output_folder=tmp_path.as_posix(),
                                                    gtfs_metadata_file=Path('tests/resources/gtfs_extract_assets/'
                                                                            '.gtfs_metadata.json').as_posix())

        assert tmp_path.joinpath(ROUTE_STAT_FILE_NAME).is_file()
        assert tmp_path.joinpath(TRIP_STAT_FILE_NAME).is_file()
        assert (trip_stats.shape, route_stats.shape) == ((74, 49), (3, 58))

    def test_get_archive_folder_path_relative_path(self):
        base = Path("./foo/bar")
        date = datetime.date(1999, 5, 29)
        expected = Path("./foo/bar/1999/5/29")
        actual = get_archive_folder_path(base, date)
        assert actual == expected

    def test_get_archive_folder_path_abs_path(self):
        base = Path("/home/foo/bar")
        date = datetime.date(1999, 5, 29)
        expected = Path("/home/foo/bar/1999/5/29")
        actual = get_archive_folder_path(base, date)
        assert actual == expected

    def test_analyze_gtfs_stat_with_output_folder_with_a_dot_in_name(self, tmp_path):
        """
        Test a use case described in https://github.com/hasadna/open-bus/issues/345
        """

        tmp_path = tmp_path.joinpath('.data')
        os.mkdir(tmp_path)

        trip_stats, route_stats = analyze_gtfs_stat(date_to_analyze=datetime.datetime(2019, 3, 7),
                                                    output_folder=tmp_path.as_posix(),
                                                    gtfs_metadata_file=Path('./tests/resources/gtfs_extract_assets/'
                                                                            '.gtfs_metadata.json').as_posix())

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
