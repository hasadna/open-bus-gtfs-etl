
from open_bus_gtfs_etl import config
from open_bus_gtfs_etl.archives import Archives


_archives = Archives(root_archives_folder=config.GTFS_ETL_ROOT_ARCHIVES_FOLDER)


def cleanup_dated_paths(num_days_keep, num_weeklies_keep):
    print('gtfs cleanup')
    _archives.gtfs.cleanup_dated_path(num_days_keep, num_weeklies_keep)
    print('stat cleanup')
    _archives.stat.cleanup_dated_path(num_days_keep, num_weeklies_keep)
