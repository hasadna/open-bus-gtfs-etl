import click

from . import (
    download_extract as download_extract_api,
    load_stops_to_db as load_stops_to_db_api,
    load_routes_to_db as load_routes_to_db_api,
    load_trips_to_db as load_trips_to_db_api,
    load_stop_times_to_db as load_stop_times_to_db_api,
    cleanup_dated_paths as cleanup_dated_paths_api,
    cleanup_workdir as cleanup_workdir_api,
)


@click.group()
def main():
    pass


@main.command()
@click.option('--from-mot', is_flag=True)
@click.option('--from-stride', is_flag=True)
@click.option('--date', type=str,
              help="Date string (%Y-%m-%d). If provided will attempt to download old data from stride project. "
                   "If not provided will download latest data for current date from MOT.")
@click.option('--force-download', is_flag=True,
              help="Force download of data, even if data already exists locally")
@click.option('--num-retries',
              help="Number of retries to make in case of problems, relevant only with --from-mot, defaults to 10 retries")
@click.option('--retry-sleep-seconds', default=120)
def download_extract(**kwargs):
    """Downloads the daily gtfs data, store in a directory structure: gtfs_data/YEAR/MONTH/DAY
    and extract to dated workdir"""
    download_extract_api.main(**kwargs)


@main.command()
@click.option('--date', type=str, help="Date string (%Y-%m-%d) to analyze. If not provided uses current date")
def load_stops_to_db(**kwargs):
    """Must run after extract command - loads the gtfs stops to DB from workdir"""
    load_stops_to_db_api.main(**kwargs)


@main.command()
@click.option('--date', type=str, help="Date string (%Y-%m-%d) to analyze. If not provided uses current date")
def load_routes_to_db(**kwargs):
    """Must run after extract command - loads the gtfs routes to DB from workdir"""
    load_routes_to_db_api.main(**kwargs)


@main.command()
@click.option('--date', type=str, help="Date string (%Y-%m-%d) to analyze. If not provided uses current date")
def load_trips_to_db(**kwargs):
    """Must run after load-routes-to-db -
    loads the gtfs trips to DB from workdir and combines with routes in DB"""
    load_trips_to_db_api.main(**kwargs)


@main.command()
@click.option('--date', type=str, help="Date string (%Y-%m-%d) to analyze. If not provided uses current date")
@click.option('--limit', type=int, help="Limit the number of rows to process (for debugging)")
@click.option('--debug', is_flag=True, help="Output debugging details (should be used with limit to prevent flood of logs)")
def load_stop_times_to_db(**kwargs):
    """Must run after load-trips-to-db and load-stops-to-db -
    loads the gtfs stop_times to DB and combines with rides and stops in DB"""
    load_stop_times_to_db_api.main(**kwargs)


@main.command()
@click.option('--num-days-keep', default=5, help='keeps a directory per day for this many last days')
@click.option('--num-weeklies-keep', default=4, help='keeps a single directory per week for this many weeks')
def cleanup_dated_paths(**kwargs):
    """Delete old directories from the dated paths"""
    cleanup_dated_paths_api.main(**kwargs)


@main.command()
@click.option('--date', type=str, help="Date string (%Y-%m-%d) to cleanup. If not provided uses current date")
def cleanup_workdir(**kwargs):
    """Deleted the dated workdir after all work was done"""
    cleanup_workdir_api.main(**kwargs)
