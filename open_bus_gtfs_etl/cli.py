import datetime
from pathlib import Path

import click

from open_bus_gtfs_etl.gtfs_extractor.gtfs_extractor import GTFSFiles, GTFS_METADATA_FILE
from open_bus_gtfs_etl.main import download_gtfs_files, write_gtfs_metadata_into_file, analyze_gtfs_stat, main


@click.group()
def entry_point():
    pass


@entry_point.command('download-gtfs', help="Download GTFS file from MOT FTP server")
@click.argument('outputs_folder')
def download_gtfs_files_cli(outputs_folder: Path) -> GTFSFiles:
    # pylint: disable=unused-argument
    return download_gtfs_files(**locals())


@entry_point.command('create-gtfs-metadata', help="Create metadata file of existed GTFS files")
@click.option('--output', required=True, type=click.Path(path_type=Path))
@click.option('--gtfs', required=True, type=click.Path(path_type=Path, file_okay=True))
@click.option('--tariff', 'tariff', required=True, type=click.Path(path_type=Path, file_okay=True))
@click.option('--cluster-to-line', 'cluster_to_line', required=True, type=click.Path(path_type=Path, file_okay=True))
@click.option('--trip-id-to-date', 'trip_id_to_date', required=True, type=click.Path(path_type=Path, file_okay=True))
def write_gtfs_metadata_into_file_cli(output: Path, gtfs: Path, tariff: Path, cluster_to_line: Path,
                                      trip_id_to_date: Path):
    # pylint: disable=unused-argument
    write_gtfs_metadata_into_file(**locals())


@entry_point.command('analyze-gtfs-stat', help="Analyze GTFS files into stat route and trips")
@click.option('--gtfs-metadata-file', type=click.Path(path_type=Path),
              help=f'in case a folder is provided, will look for the '
                   f'default metadata file name: {GTFS_METADATA_FILE}', required=True)
@click.option('--date-to-analyze', type=click.DateTime(formats=["%Y-%m-%d"]),
              default=str(datetime.date.today()))
@click.option('--output-folder', required=True, type=click.Path(path_type=Path))
def analyze_gtfs_stat_cli(output_folder: Path, date_to_analyze: datetime.datetime, gtfs_metadata_file: Path = None):
    # pylint: disable=unused-argument
    analyze_gtfs_stat(**locals())


@entry_point.command("upload-gtfs-stat-to-db", help="Main endpoint for GTFS ETL.")
@click.option('--gtfs-metadata-file', type=click.Path(path_type=Path),
              help=f'in case a folder is provided, will look for the '
                   f'default metadata file name: {GTFS_METADATA_FILE}')
@click.option('--date-to-analyze', type=click.DateTime(formats=["%Y-%m-%d"]),
              default=str(datetime.date.today()), help="Date to create filtered stat analysis if not provided")
@click.option('--route-stat-file', type=click.Path(path_type=Path), default=None,
              help="path to route stat file, product of analyze-gtfs-stat")
def basic_command(gtfs_metadata_file: Path = None, date_to_analyze: datetime.datetime = None,
                  route_stat_file: Path = None):
    # pylint: disable=unused-argument
    main(**locals())


if __name__ == '__main__':
    entry_point()
