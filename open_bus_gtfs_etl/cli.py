import datetime
from pathlib import Path
from textwrap import dedent

import click

from open_bus_gtfs_etl import api
from open_bus_gtfs_etl.api import download_gtfs_files_into_archive_folder, analyze_gtfs_stat_into_archive_folder, \
    load_analyzed_gtfs_stat_from_archive_folder


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
def analyze_gtfs_stat(gtfs_metadata_file, output_folder, **kwargs):
    "Analyze GTFS files into stat route and trips"
    api.analyze_gtfs_stat(gtfs_metadata_file=gtfs_metadata_file.decode() if gtfs_metadata_file else None,
                          output_folder=output_folder.decode() if output_folder else None,
                          **kwargs)


@main.command()
@click.option('--gtfs-metadata-file', type=click.Path(path_type=Path),
              help=f'in case a folder is provided, will look for the '
                   f'default metadata file name: {api.GTFS_METADATA_FILE}')
@click.option('--date-to-analyze', type=click.DateTime(formats=["%Y-%m-%d"]),
              default=str(datetime.date.today()), help="Date to create filtered stat analysis if not provided")
@click.option('--route-stat-file', type=click.Path(path_type=Path), default=None,
              help="path to route stat file, product of analyze-gtfs-stat")
def upload_gtfs_stat_to_db(**kwargs):
    """Main entrypoint for GTFS ETL."""
    api.main(**kwargs)


@main.command()
def download_gtfs_into_archive():
    download_gtfs_files_into_archive_folder()


@main.command()
@click.option('--date-to-analyze', type=click.DateTime(formats=["%Y-%m-%d"]),
              default=str(datetime.date.today()), help="Date to create filtered stat analysis if not provided")
def analyze_gtfs_into_archive(date_to_analyze: datetime.datetime):
    analyze_gtfs_stat_into_archive_folder(date_to_analyze.date())


@main.command()
@click.option('--date-to-upload', type=click.DateTime(formats=["%Y-%m-%d"]),
              default=str(datetime.date.today()), help="Date to create filtered stat analysis if not provided")
def upload_analyze_gtfs_from_archive(date_to_upload: datetime.datetime):
    load_analyzed_gtfs_stat_from_archive_folder(date_to_upload.date())


if __name__ == '__main__':
    main()
