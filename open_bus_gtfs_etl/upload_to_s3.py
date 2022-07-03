import os
import datetime
import subprocess

from . import common, config


def upload_rename(local_filepath, date, basename, ext, force):
    assert config.OPEN_BUS_STRIDE_PUBLIC_S3_WRITE_ACCESS_KEY_ID and config.OPEN_BUS_STRIDE_PUBLIC_S3_WRITE_SECRET_ACCESS_KEY
    target_s3_path = common.get_s3_dated_path(date, f'{basename}.{ext}')
    print(f'target_s3_path: {target_s3_path}')
    if subprocess.call(
            [
                'aws', 's3', 'ls', target_s3_path
            ],
            env={**os.environ,
                 'AWS_ACCESS_KEY_ID': config.OPEN_BUS_STRIDE_PUBLIC_S3_WRITE_ACCESS_KEY_ID,
                 'AWS_SECRET_ACCESS_KEY': config.OPEN_BUS_STRIDE_PUBLIC_S3_WRITE_SECRET_ACCESS_KEY}
    ) == 0 and not force:
        print(f"File already exists in S3, will not overwrite")
    else:
        subprocess.check_call(
            [
                'aws', 's3', 'cp',
                local_filepath, target_s3_path
            ],
            env={**os.environ,
                 'AWS_ACCESS_KEY_ID': config.OPEN_BUS_STRIDE_PUBLIC_S3_WRITE_ACCESS_KEY_ID,
                 'AWS_SECRET_ACCESS_KEY': config.OPEN_BUS_STRIDE_PUBLIC_S3_WRITE_SECRET_ACCESS_KEY}
        )


def main_upload_all(force=False):
    for i in range(9999):
        date = (datetime.datetime.now() - datetime.timedelta(days=i))
        if os.path.exists(common.get_dated_path(date)):
            main(date, force)


def main(date=None, force=False, upload_all=False, upload_date=None, archive_folder=None):
    if upload_all:
        assert not archive_folder
        assert not upload_date
        assert not date
        main_upload_all(force)
    else:
        date = common.parse_date_str(date)
        if archive_folder:
            base_path = archive_folder
        else:
            base_path = common.get_dated_path(date)
        if not upload_date:
            upload_date = date
        print(f"Uploading from local path '{base_path}' to s3 path '{common.get_s3_dated_path(upload_date)}'")
        basenames = ['ClusterToLine', 'Tariff', 'TripIdToDate', 'israel-public-transportation']
        for basename in basenames:
            file_path = os.path.join(base_path, f'{basename}.zip')
            if os.path.exists(file_path):
                upload_rename(file_path, upload_date, basename, 'zip', force)
            else:
                print(f"WARNING! missing file: {basename}.zip")
