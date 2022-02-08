import os
import datetime

from . import common, config, gtfs_extractor


def from_mot():
    date: datetime.date = datetime.date.today()
    archive_folder = common.get_dated_path(date)
    print("Downloading GTFS files to archive folder: {}".format(archive_folder))
    gtfs_extractor.GtfsRetriever(archive_folder).retrieve_gtfs_files()
    return date


def from_stride(date, force_download):
    date = common.parse_date_str(date)
    assert date, 'must provide date or download analyzed data'
    base_url = f'https://open-bus-gtfs-data.hasadna.org.il/gtfs_archive/{date.strftime("%Y/%m/%d")}/'
    base_path = os.path.join(config.GTFS_ETL_ROOT_ARCHIVES_FOLDER, 'gtfs_archive', date.strftime('%Y/%m/%d'))
    print(f"Downloading GTFS files from {base_url} to {base_path}")
    for filename in ['ClusterToLine.zip', 'Tariff.zip', 'TripIdToDate.zip', 'israel-public-transportation.zip']:
        url = base_url + filename
        path = os.path.join(base_path, filename)
        if os.path.exists(path) and not force_download:
            print("File already exists: {}".format(path))
        else:
            common.http_stream_download(path, url=url)
    return date
