from pathlib import Path

from . import common, config
from .gtfs_stat.gtfs_stats import ROUTE_STAT_FILE_NAME
from .gtfs_loader import Loader


def main(workdir: str):
    workdir = common.get_workdir(workdir)
    route_stat_file = Path(workdir, config.WORKDIR_ANALYZED_OUTPUT, ROUTE_STAT_FILE_NAME)
    if not route_stat_file.is_file():
        raise common.UserError(f"Can't find relevant route stat file at {route_stat_file}. "
                               f"Please check that you analyze gtfs files first.")
    with common.print_memory_usage('Upserting routes...'):
        Loader(route_stat_file).upsert_routes()
