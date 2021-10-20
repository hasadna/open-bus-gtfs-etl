import click

from open_bus_gtfs_etl import api
from .stop_times import cli as stop_times_cli


@click.group()
def main():
    pass


main.add_command(stop_times_cli.stop_times)


@main.command()
@click.option('--date', type=str,
              help="Date string (%Y-%m-%d). If provided will attempt to download old data from stride project. "
                   "If not provided will download latest data for current date from MOT.")
def download(date):
    """Downloads the daily gtfs data and store in a directory structure: gtfs_data/YEAR/MONTH/DAY"""
    if date:
        api.download_gtfs_files_from_stride(date)
    else:
        api.download_gtfs_files_into_archive_folder()


@main.command()
@click.option('--date', type=str,
              help="Date string (%Y-%m-%d) to create filtered stat analysis. "
                   "If not provided uses current date")
def analyze(**kwargs):
    """Analyzes the daily gtfs data from gtfs_data/ and stores in a directory structure: stat_data/YEAR/MONTH/DAY"""
    api.analyze_gtfs_stat_into_archive_folder(**kwargs)


@main.command()
@click.option('--date', type=str,
              help="Date string (%Y-%m-%d) to create filtered stat analysis. "
                   "if not provided uses current date")
def load_to_db(**kwargs):
    """Load the analyzed data from stat_data/ into the database"""
    api.load_analyzed_gtfs_stat_from_archive_folder(**kwargs)


@main.command()
@click.option('--num-days-keep', default=5, help='keeps a directory per day for this many last days')
@click.option('--num-weeklies-keep', default=4, help='keeps a single directory per week for this many weeks')
def cleanup_dated_paths(**kwargs):
    """Delete old directories from the dated paths"""
    api.cleanup_dated_paths(**kwargs)


if __name__ == '__main__':
    main()
