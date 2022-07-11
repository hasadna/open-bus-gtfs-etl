import os
from pathlib import Path


# the local folder that will contain a sub folder "gtfs_archive"
# for downloaded gtfs files from MOT and "stat_archive" for analyzed files that could be uploaded to db.
GTFS_ETL_ROOT_ARCHIVES_FOLDER = Path(os.environ.get('GTFS_ETL_ROOT_ARCHIVES_FOLDER') or '.data')

GTFS_ARCHIVE_FOLDER = 'gtfs_archive'

WORKDIR_ISRAEL_PUBLIC_TRANSPORTATION = 'israel-public-transportation'
WORKDIR_TARIFF = 'Tariff'
WORKDIR_CLUSTER_TO_LINE = 'ClusterToLine'
WORKDIR_TRIP_ID_TO_DATE = 'TripIdToDate'
WORKDIR_ANALYZED_OUTPUT = 'analyzed'

OPEN_BUS_STRIDE_PUBLIC_S3_BUCKET_NAME = os.environ.get('OPEN_BUS_STRIDE_PUBLIC_S3_BUCKET_NAME') or 'openbus-stride-public'
OPEN_BUS_STRIDE_PUBLIC_S3_WRITE_ACCESS_KEY_ID = os.environ.get('OPEN_BUS_STRIDE_PUBLIC_S3_WRITE_ACCESS_KEY_ID')
OPEN_BUS_STRIDE_PUBLIC_S3_WRITE_SECRET_ACCESS_KEY = os.environ.get('OPEN_BUS_STRIDE_PUBLIC_S3_WRITE_SECRET_ACCESS_KEY')
OPEN_BUS_STRIDE_PUBLIC_S3_OBJECT_PREFIX = os.environ.get('OPEN_BUS_STRIDE_PUBLIC_S3_OBJECT_PREFIX') or 'tests'
