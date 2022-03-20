import shutil
import datetime
from pathlib import Path
from collections import defaultdict

from . import config, common


def delete_dated_path(date, silent=False):
    if not silent:
        print("Delete: {}".format(date))
    shutil.rmtree(common.get_dated_path(date))


def iterate_dated_path_dates():
    root_folder = Path(config.GTFS_ETL_ROOT_ARCHIVES_FOLDER, config.GTFS_ARCHIVE_FOLDER)
    for path in root_folder.rglob('**'):
        parts = str(path.relative_to(root_folder)).split('/')
        if len(parts) == 3:
            try:
                year, month, day = map(int, parts)
            except Exception:
                pass
            else:
                yield datetime.date(year, month, day)


def main(num_days_keep, num_weeklies_keep, silent=False):
    stats = defaultdict(int)
    weekly_dates = []
    delete_dates = []
    for date in iterate_dated_path_dates():
        if date + datetime.timedelta(days=num_days_keep) >= datetime.date.today():
            stats['Keep dates within number of last days to keep'] += 1
        elif date + datetime.timedelta(days=num_weeklies_keep * 7) <= datetime.date.today():
            stats['Delete dates older then number of weeklies to keep'] += 1
            delete_dates.append(date)
        else:
            weekly_dates.append(date)
    for date in delete_dates:
        delete_dated_path(date, silent=silent)
    last_date = None
    for date in sorted(weekly_dates):
        if last_date is None or last_date + datetime.timedelta(days=7) <= date:
            stats['Keep weekly dates'] += 1
            last_date = date
        else:
            stats['Delete dates not kept in weeklies'] += 1
            delete_dated_path(date, silent=silent)
    if not silent:
        print(dict(stats))
