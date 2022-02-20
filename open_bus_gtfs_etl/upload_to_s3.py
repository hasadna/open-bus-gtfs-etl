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


def main(date=None, force=False, upload_all=False):
    if upload_all:
        assert not date
        main_upload_all(force)
    else:
        date = common.parse_date_str(date)
        print(f"Uploading from local path '{common.get_dated_path(date)}' to s3 path '{common.get_s3_dated_path(date)}'")
        basenames = ['ClusterToLine', 'Tariff', 'TripIdToDate', 'israel-public-transportation']
        for basename in basenames:
            if os.path.exists(common.get_dated_path(date, f'{basename}.zip')):
                upload_rename(common.get_dated_path(date, f'{basename}.zip'), date, basename, 'zip', force)
            else:
                print(f"WARNING! missing file: {basename}.zip")
