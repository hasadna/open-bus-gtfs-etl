import tempfile
import datetime
import traceback
from pprint import pprint
from collections import defaultdict

from open_bus_stride_db.db import get_session
from open_bus_stride_db.model import GtfsData

from . import common, download_extract_upload


def gtfs_data_download_upload_started(date):
    with get_session() as session:
        gtfs_data = session.query(GtfsData).filter(GtfsData.date == date).one_or_none()
        if gtfs_data is None:
            gtfs_data = GtfsData(
                date=date,
                download_upload_started_at=datetime.datetime.now(datetime.timezone.utc)
            )
            session.add(gtfs_data)
            session.commit()
            return gtfs_data.id
        else:
            gtfs_data.download_upload_started_at = datetime.datetime.now(datetime.timezone.utc)
            gtfs_data.download_upload_completed_at = None
            gtfs_data.download_upload_error = None
            gtfs_data.download_upload_success = None
            session.commit()
            return gtfs_data.id


def update_gtfs_data(gtfs_data_id, error=None, success=None):
    with get_session() as session:
        gtfs_data = session.query(GtfsData).filter(GtfsData.id == gtfs_data_id).one_or_none()
        assert gtfs_data
        gtfs_data.download_upload_completed_at = datetime.datetime.now(datetime.timezone.utc)
        if success:
            assert not error
            gtfs_data.download_upload_success = True
        else:
            assert error
            gtfs_data.download_upload_success = False
            gtfs_data.download_upload_error = error
        session.commit()


# MOT publishes the GTFS file for date D on the evening of D-1. The files are generated between
# 19:28 and 23:00 Israel time (zip timestamps, 2025-10..2026-10), but can go live hours later -
# the file generated at 20:06 on 2026-10-08 was still not served at 23:28. From this hour on we
# try to download the next day's file, and retry hourly until MOT serves it (see expected_date
# in download_extract_upload), so the day's plan is processed as soon as it is available.
# Earlier hours keep targeting today, which covers a missed evening.
NEXT_DAY_FROM_HOUR = 20


def get_date_to_download():
    now = common.israel_now()
    if now.hour >= NEXT_DAY_FROM_HOUR:
        return now.date() + datetime.timedelta(days=1)
    return now.date()


def main():
    """This task is idempotent and makes sure that the latest GTFS data is available:
    today's, and from the evening the next day's, which MOT publishes the evening before.
    It uses DB gtfs_data table to keep track. It downloads the data from MOT and uploads it to S3.
    """
    stats = defaultdict(int)
    date = get_date_to_download()
    needs_download_upload_from_mot = False
    with get_session() as session:
        if session.query(GtfsData).filter(GtfsData.date == date, GtfsData.download_upload_success == True).count() < 1:
            needs_download_upload_from_mot = True
    if needs_download_upload_from_mot:
        print(f'Uploaded data is missing for {date}, will download / upload from MOT')
        with tempfile.TemporaryDirectory() as workdir:
            gtfs_data_id = gtfs_data_download_upload_started(date)
            try:
                stats['download_upload_from_mot'] += 1
                download_extract_upload.main(from_mot=True, target_path=workdir, expected_date=date)
            except download_extract_upload.MotFileNotForDate as e:
                # MOT hasn't published the file yet - expected on late evenings, the next hourly run retries
                print(f'{e}, will try again on the next run')
                stats['mot_file_not_published_yet'] += 1
                update_gtfs_data(gtfs_data_id, error=str(e))
            except:
                update_gtfs_data(gtfs_data_id, error=traceback.format_exc())
                raise
            else:
                update_gtfs_data(gtfs_data_id, success=True)
    else:
        print(f'Uploaded data is already available for {date}, will not download / upload from MOT')
    pprint(dict(stats))
    print('OK')
