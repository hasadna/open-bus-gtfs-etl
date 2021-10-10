import datetime

import click

from open_bus_gtfs_etl import api


@click.group()
def main():
    pass


@main.command()
def download():
    """Downloads the daily gtfs data and store in a directory structure: gtfs_data/YEAR/MONTH/DAY"""
    api.download_gtfs_files_into_archive_folder()


@main.command()
@click.option('--date-to-analyze', type=click.DateTime(formats=["%Y-%m-%d"]),
              default=str(datetime.date.today()), help="Date to create filtered stat analysis. "
                                                       "If not provided uses current date")
def analyze(date_to_analyze: datetime.datetime):
    """Analyzes the daily gtfs data from gtfs_data/ and stores in a directory structure: stat_data/YEAR/MONTH/DAY"""
    api.analyze_gtfs_stat_into_archive_folder(date_to_analyze.date())


@main.command()
@click.option('--date-to-upload', type=click.DateTime(formats=["%Y-%m-%d"]),
              default=str(datetime.date.today()), help="Date to create filtered stat analysis. "
                                                       "if not provided uses current date")
def load_to_db(date_to_upload: datetime.datetime):
    """Load the analyzed data from stat_data/ into the database"""
    api.load_analyzed_gtfs_stat_from_archive_folder(date_to_upload.date())


@main.command()
@click.option('--num-days-keep', default=5, help='keeps a directory per day for this many last days')
@click.option('--num-weeklies-keep', default=4, help='keeps a single directory per week for this many weeks')
def cleanup_dated_paths(**kwargs):
    """Delete old directories from the dated paths"""
    api.cleanup_dated_paths(**kwargs)
