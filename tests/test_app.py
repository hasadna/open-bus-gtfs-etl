import datetime
import os
from pathlib import Path
from typing import Optional
from open_bus_stride_db.db import session_decorator
from sqlalchemy.orm import Session

from open_bus_gtfs_etl.gtfs_extractor.gtfs_extractor import GtfsRetriever, GTFS_EXTRACTOR_CONFIG, GTFSFiles
from open_bus_gtfs_etl.gtfs_loader import RouteRecord
from open_bus_gtfs_etl.gtfs_stat.gtfs_stats import create_trip_and_route_stat
from open_bus_gtfs_etl.main import main


@session_decorator
def create_route_stat_items(session: Session, date_to_analyze: datetime.date, gtfs_file_path: Path, tariff_file_path: Path,
                            cluster_to_line_file_path: Path, trip_id_to_date_file_path: Path,
                            output_folder: Optional[Path] = None):

    trip_stat, route_stat = create_trip_and_route_stat(date_to_analyze, gtfs_file_path, tariff_file_path,
                                                       cluster_to_line_file_path, trip_id_to_date_file_path,
                                                       output_folder)

    for _, route in route_stat.iterrows():
        session.add(RouteRecord.from_row(route).convert_into_db_route())

    session.commit()
    print(session)


class TestGtgsStat:
    def test_sample_run(self, tmp_path: Path):

        base = Path(__file__).parent.joinpath('resources', 'gtfs_stat_assets')
        create_trip_and_route_stat(date_to_analyze=datetime.date(2019, 3, 7),
                                   gtfs_file_path=base.joinpath("2019-03-07-israel-public-transportation.zip"),
                                   tariff_file_path=base.joinpath("2019-03-07-Tariff.zip"),
                                   cluster_to_line_file_path=base.joinpath("2019-03-07-ClusterToLine.zip"),
                                   trip_id_to_date_file_path=base.joinpath("2019-03-07-TripIdToDate.zip"),
                                   output_folder=tmp_path)

        assert tmp_path.joinpath('route_stats.csv.gz').is_file()
        assert tmp_path.joinpath('trip_stats.csv.gz').is_file()

    def test_gtfs_stat_into_objs(self, tmp_path: Path):
        base = Path(__file__).parent.joinpath('resources', 'gtfs_stat_assets')

        create_route_stat_items(date_to_analyze=datetime.date(2019, 3, 7),
                                gtfs_file_path=base.joinpath("2019-03-07-israel-public-transportation.zip"),
                                tariff_file_path=base.joinpath("2019-03-07-Tariff.zip"),
                                cluster_to_line_file_path=base.joinpath("2019-03-07-ClusterToLine.zip"),
                                trip_id_to_date_file_path=base.joinpath("2019-03-07-TripIdToDate.zip"),
                                output_folder=tmp_path)


class TestGtfsExtractor:
    def test_retrieve_gtfs_files_without_download(self, tmp_path: Path):
        tmp_dir = tmp_path.as_posix()

        retriever = GtfsRetriever(folder=Path(tmp_dir))
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


class TestMain:
    def test_main(self, tmp_path: Path):
        base = Path(__file__).parent.joinpath('resources', 'gtfs_stat_assets')
        gtfs_files = GTFSFiles(
            gtfs=Path(base, '2019-03-07-israel-public-transportation.zip'),
            tariff=Path(base, '2019-03-07-Tariff.zip'),
            cluster_to_line=Path(base, '2019-03-07-ClusterToLine.zip'),
            trip_id_to_date=Path(base, '2019-03-07-TripIdToDate.zip'))

        main(outputs_folder=tmp_path, gtfs_files=gtfs_files, date_to_analyze=datetime.date(2019, 3, 7))
