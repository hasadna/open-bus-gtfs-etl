import datetime
from pathlib import Path
from textwrap import dedent

import click

from open_bus_gtfs_etl import api


@click.group()
def main():
    pass


@main.command()
@click.argument('outputs_folder')
def download_gtfs(**kwargs):
    "Download GTFS file from MOT FTP server"
    files = api.download_gtfs_files(**kwargs)
    print(dedent(f"""
        gtfs: "{files.gtfs}"
        tariff: "{files.tariff}"
        cluster_to_line: "{files.cluster_to_line}"
        trip_id_to_date: "{files.trip_id_to_date}"
    """))


@main.command()
@click.option('--output', required=True, type=click.Path(path_type=Path))
@click.option('--gtfs', required=True, type=click.Path(path_type=Path, file_okay=True))
@click.option('--tariff', 'tariff', required=True, type=click.Path(path_type=Path, file_okay=True))
@click.option('--cluster-to-line', 'cluster_to_line', required=True, type=click.Path(path_type=Path, file_okay=True))
@click.option('--trip-id-to-date', 'trip_id_to_date', required=True, type=click.Path(path_type=Path, file_okay=True))
def create_gtfs_metadata(**kwargs):
    "Create metadata file of existed GTFS files"
    api.write_gtfs_metadata_into_file(**kwargs)


@main.command('analyze-gtfs-stat', help="Analyze GTFS files into stat route and trips")
@click.option('--gtfs-metadata-file', type=click.Path(path_type=Path),
              help=f'in case a folder is provided, will look for the '
                   f'default metadata file name: {api.GTFS_METADATA_FILE}', required=True)
@click.option('--date-to-analyze', type=click.DateTime(formats=["%Y-%m-%d"]),
              default=str(datetime.date.today()))
@click.option('--output-folder', required=True, type=click.Path(path_type=Path))
def analyze_gtfs_stat(**kwargs):
    "Analyze GTFS files into stat route and trips"
    analyze_gtfs_stat(**kwargs)


@main.command()
@click.option('--gtfs-metadata-file', type=click.Path(path_type=Path),
              help=f'in case a folder is provided, will look for the '
                   f'default metadata file name: {api.GTFS_METADATA_FILE}')
@click.option('--date-to-analyze', type=click.DateTime(formats=["%Y-%m-%d"]),
              default=str(datetime.date.today()), help="Date to create filtered stat analysis if not provided")
@click.option('--route-stat-file', type=click.Path(path_type=Path), default=None,
              help="path to route stat file, product of analyze-gtfs-stat")
def upload_gtfs_stat_to_db(**kwargs):
    "Main entrypoint for GTFS ETL."
    api.main(**kwargs)
