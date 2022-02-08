import time
import traceback

from . import download, extract


def main(from_mot=False, from_stride=False, date=None, force_download=False, num_retries=None, retry_sleep_seconds=120):
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
    while not is_success and num_failures < num_retries:
        if num_failures > 0:
            print(f'failure {num_failures}/{num_retries}, will try again in {retry_sleep_seconds} seconds...')
            time.sleep(retry_sleep_seconds)
        if from_mot:
            date = download.from_mot()
        else:
            date = download.from_stride(date, force_download)
        print(f'Downloaded date: {date}, proceeding with extract..')
        try:
            extract.main(date)
            is_success = True
        except extract.ExtractUnzipException:
            traceback.print_exc()
            num_failures += 1
