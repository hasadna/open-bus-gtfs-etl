import tempfile
import datetime
import traceback
from pprint import pprint
from collections import defaultdict

from open_bus_stride_db.db import get_session
from open_bus_stride_db.model import GtfsData

from . import download_extract_upload


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


def main():
    """This task is idempotent and makes sure that latest GTFS data for today
    is available. It uses DB gtfs_data table to keep track. It downloads today's
    data from MOT and uploads it to S3.
    """
    stats = defaultdict(int)
    date = datetime.date.today()
    needs_download_upload_from_mot = False
    with get_session() as session:
        if session.query(GtfsData).filter(GtfsData.date == date, GtfsData.download_upload_success == True).count() < 1:
            needs_download_upload_from_mot = True
    if needs_download_upload_from_mot:
        print(f'Uploaded data is missing for today ({date}), will download / upload from MOT')
        with tempfile.TemporaryDirectory() as workdir:
            gtfs_data_id = gtfs_data_download_upload_started(date)
            try:
                stats['download_upload_from_mot'] += 1
                download_extract_upload.main(from_mot=True, target_path=workdir)
            except:
                update_gtfs_data(gtfs_data_id, error=traceback.format_exc())
                raise
            else:
                update_gtfs_data(gtfs_data_id, success=True)
    else:
        print(f'Uploaded data is already available for today ({date}), will not download / upload from MOT')
    pprint(dict(stats))
    print('OK')
