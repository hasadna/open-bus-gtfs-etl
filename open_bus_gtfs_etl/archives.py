import datetime
from pathlib import Path

GTFS_ARCHIVE_FOLDER = "gtfs_archive"
STAT_ARCHIVE_FOLDER = "stat_archive"


class Archive:
    def __init__(self, root_folder: Path):
        self.root_folder = root_folder

    def get_dated_path(self, date: datetime.datetime, *args):
        return self.root_folder.joinpath(str(date.year), str(date.month), str(date.day), *args)


class Archives:
    def __init__(self, root_archives_folder: Path):

        self.gtfs = Archive(root_folder=root_archives_folder.joinpath(GTFS_ARCHIVE_FOLDER))
        self.stat = Archive(root_folder=root_archives_folder.joinpath(STAT_ARCHIVE_FOLDER))
