import click

# from open_bus_gtfs_etl.stop_times.cli import stop_times
from . import (
    extract as extract_api,
    download as download_api,
    analyze as analyze_api,
    load_to_db as load_to_db_api,
    cleanup_dated_paths as cleanup_dated_paths_api
)


@click.group()
def main():
    pass


# main.add_command(stop_times)


@main.command()
@click.option('--date', type=str,
              help="Date string (%Y-%m-%d). If provided will attempt to download old data from stride project. "
                   "If not provided will download latest data for current date from MOT.")
@click.option('--analyzed', is_flag=True,
              help="If enabled, will download the analyzed data from remote stride workdir")
@click.option('--workdir', type=str,
              help="Workdir is used for input (extracted data) and output.")
def download(date, analyzed, workdir):
    """Downloads the daily gtfs data and store in a directory structure: gtfs_data/YEAR/MONTH/DAY"""
    if date or analyzed:
        download_api.from_stride(date, analyzed, workdir)
    else:
        download_api.from_mot()


@main.command()
@click.option('--date', type=str,
              help="Date string (%Y-%m-%d). Extract downloaded data for given date. "
                   "If not provided uses current date")
@click.option('--workdir', type=str,
              help="Directory to extract to, next steps can use data from this directory. "
                   "If not provided uses a default workdir directory")
def extract(**kwargs):
    extract_api.main(**kwargs)


@main.command()
@click.option('--date', type=str,
              help="Date string (%Y-%m-%d) to analyze. "
                   "If not provided uses current date")
@click.option('--workdir', type=str,
              help="Workdir is used for input (extracted data) and output.")
def analyze(**kwargs):
    """Analyzes the extracted gtfs data from workdir"""
    analyze_api.main(**kwargs)


@main.command()
@click.option('--workdir', type=str,
              help="Workdir is used for input (extracted data) and output.")
def load_to_db(**kwargs):
    """Load the analyzed data from stat_data/ into the database"""
    load_to_db_api.main(**kwargs)


@main.command()
@click.option('--num-days-keep', default=5, help='keeps a directory per day for this many last days')
@click.option('--num-weeklies-keep', default=4, help='keeps a single directory per week for this many weeks')
def cleanup_dated_paths(**kwargs):
    """Delete old directories from the dated paths"""
    cleanup_dated_paths_api.main(**kwargs)
