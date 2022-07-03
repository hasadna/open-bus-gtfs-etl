import os
import time
import traceback

from . import download, extract, upload_to_s3


def main(from_mot=False, from_stride=False, date=None, force_download=False, num_retries=None,
         retry_sleep_seconds=120, silent=False, target_path=None, upload_date=None):
    if from_mot:
        assert not from_stride, 'must choose either from_mot or from_stride, but not both'
        assert not date, 'must not specify date when choosing from_mot - it always downloads latest data'
        assert not force_download, 'cannot force download when choosing from_mot - it always downloads the latest data'
        if not num_retries:
            num_retries=10
    else:
        assert from_stride, 'must choose either from_mot or from_stride, but not both'
        assert not num_retries, 'when downloading from stride only a single retry is attempted'
        num_retries = 1
    num_retries = int(num_retries)
    retry_sleep_seconds = int(retry_sleep_seconds)
    num_failures = 0
    is_success = False
    if target_path:
        archive_folder = os.path.join(target_path, 'archive')
        extracted_workdir = os.path.join(target_path, 'extracted')
    else:
        archive_folder, extracted_workdir = None, None
    while not is_success and num_failures < num_retries:
        if num_failures > 0:
            print(f'failure {num_failures}/{num_retries}, will try again in {retry_sleep_seconds} seconds...')
            time.sleep(retry_sleep_seconds)
        if from_mot:
            date = download.from_mot(archive_folder=archive_folder)
        else:
            date = download.from_stride(date, force_download, silent=silent, archive_folder=archive_folder)
        if not silent:
            print(f'Downloaded date: {date}, proceeding with extract..')
        try:
            extract.main(date, silent=silent, archive_folder=archive_folder, extracted_workdir=extracted_workdir)
            is_success = True
        except extract.ExtractUnzipException:
            traceback.print_exc()
            num_failures += 1
    assert is_success
    if from_mot or upload_date:
        upload_to_s3.main(date, upload_date=upload_date, archive_folder=archive_folder)
    return extracted_workdir
