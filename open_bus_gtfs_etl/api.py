import os
import datetime
import sys
from pathlib import Path
import logging

from open_bus_gtfs_etl import config
from open_bus_gtfs_etl.archives import Archives
from open_bus_gtfs_etl.common import http_stream_download
from open_bus_gtfs_etl.gtfs_extractor.gtfs_extractor import GtfsRetriever, GTFSFiles, GTFS_METADATA_FILE
from open_bus_gtfs_etl.gtfs_loader import Loader
from open_bus_gtfs_etl.gtfs_stat.gtfs_stats import create_trip_and_route_stat, dump_trip_and_route_stat, \
    ROUTE_STAT_FILE_NAME

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


class UserError(Exception):
    """
    Exception that represent error caused by wrong input of user
    """


_archives = Archives(root_archives_folder=config.GTFS_ETL_ROOT_ARCHIVES_FOLDER)


def parse_date_str(date):
    """Parses a date string in format %Y-%m-%d with default of today if empty"""
    if isinstance(date, datetime.date):
        return date
    if not date:
        return datetime.date.today()
    return datetime.datetime.strptime(date, '%Y-%m-%d').date()


def download_gtfs_files_into_archive_folder(archives: Archives = _archives):
    date: datetime.date = datetime.date.today()
    archive_folder = archives.gtfs.get_dated_path(date)
    logger.info("Downloading GTFS files to archive folder: %s", archive_folder)
    download_gtfs_files(outputs_folder=archives.gtfs.get_dated_path(date))


def analyze_gtfs_stat_into_archive_folder(date: str, archives: Archives = _archives):
    date = parse_date_str(date)
    gtfs_metadata = archives.gtfs.get_dated_path(date, GTFS_METADATA_FILE)
    if not gtfs_metadata.is_file():
        raise UserError(f"Can't find relevant gtfs files for {date.isoformat()} in {gtfs_metadata}. "
                        f"Please check that you downloaded GTFS files")

    output_folder = archives.stat.get_dated_path(date)

    logger.info("analyzing GTFS files from archive folder: %s and save analyzed data in %s",
                gtfs_metadata, output_folder)

    analyze_gtfs_stat(date_to_analyze=date, gtfs_metadata_file=gtfs_metadata,
                      output_folder=output_folder)


def load_analyzed_gtfs_stat_from_archive_folder(date: str, archives: Archives = _archives):
    date = parse_date_str(date)
    route_stat_file = archives.stat.get_dated_path(date, ROUTE_STAT_FILE_NAME)
    if not route_stat_file.is_file():
        raise UserError(f"Can't find relevant route stat file at {route_stat_file}. "
                        f"Please check that you analyze gtfs files first.")
    Loader(route_stat_file).upsert_routes()


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
    routs_stat_path = None
    if gtfs_files is None:
        if gtfs_metadata_file.is_dir():
            gtfs_metadata_file = gtfs_metadata_file.joinpath(GTFS_METADATA_FILE)

        gtfs_files = GTFSFiles.parse_file(gtfs_metadata_file)

    logger.info('analyze gtfs stat - this could take some time')
    trip_stats, route_stats = create_trip_and_route_stat(date_to_analyze, gtfs_files)

    if output_folder is not None:
        routs_stat_path = dump_trip_and_route_stat(trip_stat=trip_stats, route_stat=route_stats,
                                                   output_folder=output_folder)

    return trip_stats, route_stats, routs_stat_path


def cleanup_dated_paths(num_days_keep, num_weeklies_keep):
    print('gtfs cleanup')
    _archives.gtfs.cleanup_dated_path(num_days_keep, num_weeklies_keep)
    print('stat cleanup')
    _archives.stat.cleanup_dated_path(num_days_keep, num_weeklies_keep)


def download_gtfs_files_from_stride(date):
    date = parse_date_str(date)
    assert date, 'must provide date'
    base_url = f'https://open-bus-gtfs-data.hasadna.org.il/gtfs_archive/{date.strftime("%Y/%m/%d")}/'
    base_path = os.path.join(config.GTFS_ETL_ROOT_ARCHIVES_FOLDER, 'gtfs_archive', date.strftime('%Y/%m/%d'))
    print(f"Downloading GTFS files from {base_url} to {base_path}")
    for filename in ['ClusterToLine.zip', 'Tariff.zip', 'TripIdToDate.zip', 'israel-public-transportation.zip']:
        url = base_url + filename
        path = os.path.join(base_path, filename)
        http_stream_download(path, url=url)
    base_url = f'https://open-bus-gtfs-data.hasadna.org.il/stat_archive/{date.strftime("%Y/%m/%d")}/'
    base_path = os.path.join(config.GTFS_ETL_ROOT_ARCHIVES_FOLDER, 'stat_archive', date.strftime('%Y/%m/%d'))
    print(f"Downloading GTFS files from {base_url} to {base_path}")
    for filename in ['route_stats.csv.gz', 'trip_stats.csv.gz']:
        url = base_url + filename
        path = os.path.join(base_path, filename)
        http_stream_download(path, url=url)
