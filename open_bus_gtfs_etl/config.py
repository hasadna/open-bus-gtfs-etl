import os
from pathlib import Path


# the local folder that will contain a sub folder "gtfs_archive"
# for downloaded gtfs files from MOT and "stat_archive" for analyzed files that could be uploaded to db.
GTFS_ETL_ROOT_ARCHIVES_FOLDER = Path(os.environ.get('GTFS_ETL_ROOT_ARCHIVES_FOLDER') or '.data')
