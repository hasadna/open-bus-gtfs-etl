import os
import shutil
import subprocess
from pathlib import Path

from . import common, config


class ExtractUnzipException(Exception):
    pass


def main(date):
    date = common.parse_date_str(date)
    dated_workdir = common.get_dated_workdir(date)
    print("Extracting GTFS data for date {} to workdir {}".format(date, dated_workdir))
    base_path = common.get_dated_path(date)
    gtfs_file_path = Path(base_path, 'israel-public-transportation.zip').absolute()
    tariff_file_path = Path(base_path, 'Tariff.zip').absolute()
    cluster_to_line_file_path = Path(base_path, 'ClusterToLine.zip').absolute()
    trip_id_to_date_file_path = Path(base_path, 'TripIdToDate.zip').absolute()
    for zip_file_name, extracted_rel_path in {
        gtfs_file_path: config.WORKDIR_ISRAEL_PUBLIC_TRANSPORTATION,
        tariff_file_path: config.WORKDIR_TARIFF,
        cluster_to_line_file_path: config.WORKDIR_CLUSTER_TO_LINE,
        trip_id_to_date_file_path: config.WORKDIR_TRIP_ID_TO_DATE,
    }.items():
        extracted_path = os.path.join(dated_workdir, extracted_rel_path)
        shutil.rmtree(extracted_path, ignore_errors=True)
        os.makedirs(extracted_path, exist_ok=True)
        if subprocess.call(['unzip', zip_file_name], cwd=extracted_path) != 0:
            raise ExtractUnzipException()
