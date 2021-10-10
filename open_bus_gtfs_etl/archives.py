import datetime
import shutil
from pathlib import Path
from collections import defaultdict

GTFS_ARCHIVE_FOLDER = "gtfs_archive"
STAT_ARCHIVE_FOLDER = "stat_archive"


class Archive:
    def __init__(self, root_folder: Path):
        self.root_folder = root_folder

    def get_dated_path(self, date: datetime.datetime, *args):
        return self.root_folder.joinpath(date.strftime('%Y/%m/%d'), *args)

    def iterate_dated_path_dates(self):
        for path in self.root_folder.rglob('**'):
            parts = str(path.relative_to(self.root_folder)).split('/')
            if len(parts) == 3:
                try:
                    year, month, day = map(int, parts)
                except Exception:
                    pass
                else:
                    yield datetime.date(year, month, day)

    def delete_dated_path(self, date):
        print("Delete: {}".format(date))
        shutil.rmtree(self.get_dated_path(date))

    def cleanup_dated_path(self, num_days_keep, num_weeklies_keep):
        stats = defaultdict(int)
        weekly_dates = []
        delete_dates = []
        for date in self.iterate_dated_path_dates():
            if date + datetime.timedelta(days=num_days_keep) >= datetime.date.today():
                stats['Keep dates within number of last days to keep'] += 1
            elif date + datetime.timedelta(days=num_days_keep*7) <= datetime.date.today():
                stats['Delete dates older then number of weeklies to keep'] += 1
                delete_dates.append(date)
            else:
                weekly_dates.append(date)
        for date in delete_dates:
            self.delete_dated_path(date)
        last_date = None
        for date in sorted(weekly_dates):
            if last_date is None or last_date + datetime.timedelta(days=7) <= date:
                stats['Keep weekly dates'] += 1
                last_date = date
            else:
                stats['Delete dates not kept in weeklies'] += 1
                self.delete_dated_path(date)
        print(dict(stats))


class Archives:
    def __init__(self, root_archives_folder: Path):

        self.gtfs = Archive(root_folder=root_archives_folder.joinpath(GTFS_ARCHIVE_FOLDER))
        self.stat = Archive(root_folder=root_archives_folder.joinpath(STAT_ARCHIVE_FOLDER))
