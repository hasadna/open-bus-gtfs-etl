import os
import shutil
import subprocess
from pathlib import Path

from . import common, config


class ExtractUnzipException(Exception):
    pass


def main(date, silent=False, archive_folder=None, extracted_workdir=None):
    date = common.parse_date_str(date)
    if extracted_workdir:
        dated_workdir = extracted_workdir
    else:
        dated_workdir = common.get_dated_workdir(date)
    if not silent:
        print("Extracting GTFS data for date {} to workdir {}".format(date, dated_workdir))
    if archive_folder:
        base_path = archive_folder
    else:
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
        if subprocess.call(['unzip', zip_file_name], cwd=extracted_path, **({'stdout': subprocess.DEVNULL} if silent else {})) != 0:
            if (
                date.strftime("%Y-%m-%d") in ('2023-03-25', '2023-03-26', '2023-03-27', '2023-03-28', '2023-03-29', '2023-04-01', '2023-04-02')
                and zip_file_name == gtfs_file_path
            ):
                print("WARNING! Only israel-public-transporation.zip is available for given date, other files are missing")
            else:
                raise ExtractUnzipException()
