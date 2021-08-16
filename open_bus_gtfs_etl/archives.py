import datetime
from pathlib import Path
from pydantic import BaseSettings

GTFS_ARCHIVE_FOLDER = "gtfs_archive"
STAT_ARCHIVE_FOLDER = "stat_archive"


class ArchiveSettings(BaseSettings):
    root_archives_folder: Path = Path(".data")

    class Config:
        env_prefix = 'gtfs_etl_'


archive_settings = ArchiveSettings()


class Archive:
    def __init__(self, root_folder: Path):
        self.root_folder = root_folder

    def get_dated_path(self, date: datetime.datetime, *args):
        return self.root_folder.joinpath(str(date.year), str(date.month), str(date.day), *args)


class Archives:
    gtfs = Archive(root_folder=archive_settings.root_archives_folder.joinpath(GTFS_ARCHIVE_FOLDER))
    stat = Archive(root_folder=archive_settings.root_archives_folder.joinpath(STAT_ARCHIVE_FOLDER))
