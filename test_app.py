import datetime
import os
import tempfile
from pathlib import Path

from gtfs_extractor.gtfs_extractor import GtfsRetriever, GtfsExtractorConfig, GTFS_EXTRACTOR_CONFIG
from gtfs_stat.gtfs_stats import create_trip_and_route_stat




class TestGtgsStat:
    def test_sample_run(self, tmp_path: Path):

        base = "resources/gtfs_stat_assets"
        create_trip_and_route_stat(date_to_analyze=datetime.date(2019, 3, 7),
                                   gtfs_file_name=os.path.join(base, "2019-03-07-israel-public-transportation.zip"),
                                   tariff_zip_name=os.path.join(base, "2019-03-07-Tariff.zip"),
                                   cluster_to_line_zip_name=os.path.join(base, "2019-03-07-ClusterToLine.zip"),
                                   trip_id_to_date_zip_name=os.path.join(base, "2019-03-07-TripIdToDate.zip"),
                                   output_folder=tmp_path.as_posix())

        assert tmp_path.joinpath('route_stats.csv.gz').is_file()
        assert tmp_path.joinpath('trip_stats.csv.gz').is_file()


class TestGtfsExtractor:
    def test_retrieve_gtfs_files_without_download(self, tmp_path: Path):
        tmp_dir = tmp_path.as_posix()

        retriever = GtfsRetriever(folder=tmp_dir)
        retriever.download_file_from_ftp = lambda url, local_file: None
        actual = retriever.retrieve_gtfs_files()

        assert actual.gtfs == Path(tmp_dir, GTFS_EXTRACTOR_CONFIG.gtfs_file.local_name)
        assert actual.tariff == Path(tmp_dir, GTFS_EXTRACTOR_CONFIG.tariff_file.local_name)
        assert actual.cluster_to_line == Path(tmp_dir, GTFS_EXTRACTOR_CONFIG.cluster_file.local_name)
        assert actual.trip_id_to_date == Path(tmp_dir, GTFS_EXTRACTOR_CONFIG.trip_id_to_date_file.local_name)

    def test_config(self, tmp_path: Path):
        local_file = tmp_path.joinpath('test.zip')
        GtfsRetriever.download_file_from_ftp(GTFS_EXTRACTOR_CONFIG.tariff_file.url, local_file)
        assert os.path.isfile(local_file.as_posix())


