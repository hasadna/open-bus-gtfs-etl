import datetime
import shutil
import sys
from pathlib import Path
from tempfile import mkdtemp
import logging

from open_bus_gtfs_etl.archives import Archives
from open_bus_gtfs_etl.gtfs_extractor.gtfs_extractor import GtfsRetriever, GTFSFiles, GTFS_METADATA_FILE
from open_bus_gtfs_etl.gtfs_loader import load_routes_to_db
from open_bus_gtfs_etl.gtfs_stat.gtfs_stats import create_trip_and_route_stat, dump_trip_and_route_stat, \
    ROUTE_STAT_FILE_NAME
from open_bus_gtfs_etl.gtfs_stat.output import read_stat_file

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


class UserError(Exception):
    pass


def download_gtfs_files_into_archive_folder(date: datetime.date):
    archive_folder = Archives.gtfs.get_dated_path(date)
    logger.info("Downloading GTFS files to archive folder: %s", archive_folder)
    download_gtfs_files(outputs_folder=Archives.gtfs.get_dated_path(date))


def analyze_gtfs_stat_into_archive_folder(date: datetime.date):
    gtfs_metadata = Archives.gtfs.get_dated_path(date, GTFS_METADATA_FILE)
    if not gtfs_metadata.is_file():
        raise UserError(f"Can't find relevant gtfs files for {date.isoformat()}. "
                        f"Please check that you downloaded GTFS files")

    output_folder = Archives.stat.get_dated_path(date)

    logger.info("analyzing GTFS files from archive folder: %s and save analyzed data in %s",
                gtfs_metadata, output_folder)

    analyze_gtfs_stat(date_to_analyze=date, gtfs_metadata_file=gtfs_metadata,
                      output_folder=output_folder)


def load_analyzed_gtfs_stat_from_archive_folder(date: datetime.date):
    route_stat_file = Archives.stat.get_dated_path(date, ROUTE_STAT_FILE_NAME)
    if not route_stat_file.is_file():
        raise UserError(f"Can't find relevant route stat file at {route_stat_file}. "
                        f"Please check that you analyze gtfs files first.")
    main(route_stat_file=route_stat_file)


def download_gtfs_files(outputs_folder: Path) -> GTFSFiles:
    logger.info('Downloading GTFS files into: %s', outputs_folder)
    return GtfsRetriever(outputs_folder).retrieve_gtfs_files()


def write_gtfs_metadata_into_file(output: Path, gtfs: Path, tariff: Path, cluster_to_line: Path,
                                  trip_id_to_date: Path):
    gtfs_files = GTFSFiles(gtfs=gtfs, tariff=tariff, cluster_to_line=cluster_to_line,
                           trip_id_to_date=trip_id_to_date)

    if output.is_dir():
        output = output.joinpath(GTFS_METADATA_FILE)

    with output.open('w') as f:
        f.write(gtfs_files.json(indent=4))


def analyze_gtfs_stat(date_to_analyze: datetime.date, gtfs_metadata_file: Path = None, output_folder: Path = None,
                      gtfs_files: GTFSFiles = None):
    if gtfs_files is None:
        if gtfs_metadata_file.is_dir():
            gtfs_metadata_file = gtfs_metadata_file.joinpath(GTFS_METADATA_FILE)

        gtfs_files = GTFSFiles.parse_file(gtfs_metadata_file)

    logger.info('analyze gtfs stat - this could take some minutes')
    trip_stats, route_stats = create_trip_and_route_stat(date_to_analyze, gtfs_files)

    if output_folder is not None:
        dump_trip_and_route_stat(trip_stat=trip_stats, route_stat=route_stats, output_folder=output_folder)

    return trip_stats, route_stats


def main(gtfs_metadata_file: Path = None, date_to_analyze: datetime.datetime = None, route_stat_file: Path = None):

    tmp_folder = None

    try:
        if route_stat_file is None:
            if date_to_analyze is None:
                date_to_analyze = datetime.datetime.today().date()
            if gtfs_metadata_file is None:
                tmp_folder = mkdtemp()
                gtfs_files = download_gtfs_files(Path(tmp_folder))

                _trip_stat, route_stat = analyze_gtfs_stat(date_to_analyze=date_to_analyze,
                                                           gtfs_files=gtfs_files)
            else:
                _trip_stat, route_stat = analyze_gtfs_stat(date_to_analyze=date_to_analyze,
                                                           gtfs_metadata_file=gtfs_metadata_file)

        else:
            route_stat = read_stat_file(path=route_stat_file)

        load_routes_to_db(route_stat=route_stat)

    finally:
        if tmp_folder is not None:
            shutil.rmtree(tmp_folder)
