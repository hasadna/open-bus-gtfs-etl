import json

import click

from . import api


@click.group()
def stop_times():
    pass


@stop_times.command('list')
@click.option('--date', type=str,
              help="Date string (%Y-%m-%d) to load the data from. "
                   "if not provided uses current date")
@click.option('--limit', default=10, help='maximum number of items to list, set to 0 to disable the limit')
def list_(**kwargs):
    for stop_time in api.list_(**kwargs):
        print(json.dumps(stop_time))


@stop_times.command()
@click.option('--date', type=str,
              help="Date string (%Y-%m-%d) to load the data from. "
                   "if not provided uses current date")
@click.option('--limit', default=0, help='limit of maximum number of items to process. '
                                         'will process all items by default, set this for debugging.')
@click.option('--no-count', is_flag=True, help='skip counting of all stop times before processing')
def load_to_db(**kwargs):
    stats: dict = api.load_to_db(**kwargs)
    for k, v in stats.items():
        print('{}: {}'.format(k, v))
