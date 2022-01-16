import os
import datetime

from . import common, config
from open_bus_gtfs_etl.gtfs_extractor.gtfs_extractor import GtfsRetriever


def from_mot():
    date: datetime.date = datetime.date.today()
    archive_folder = common.get_dated_path(date)
    print("Downloading GTFS files to archive folder: {}".format(archive_folder))
    GtfsRetriever(archive_folder).retrieve_gtfs_files()


def from_stride(date, analyzed, workdir):
    date = common.parse_date_str(date)
    workdir = common.get_workdir(workdir)
    if analyzed:
        assert not date, 'when downloading anaylzed data, the data is downloaded from remote workdir, without date'
    else:
        assert date, 'must provide date or download analyzed data'
    if date:
        base_url = f'https://open-bus-gtfs-data.hasadna.org.il/gtfs_archive/{date.strftime("%Y/%m/%d")}/'
        base_path = os.path.join(config.GTFS_ETL_ROOT_ARCHIVES_FOLDER, 'gtfs_archive', date.strftime('%Y/%m/%d'))
        print(f"Downloading GTFS files from {base_url} to {base_path}")
        for filename in ['ClusterToLine.zip', 'Tariff.zip', 'TripIdToDate.zip', 'israel-public-transportation.zip']:
            url = base_url + filename
            path = os.path.join(base_path, filename)
            common.http_stream_download(path, url=url)
    if analyzed:
        base_url = f'https://open-bus-gtfs-data.hasadna.org.il/workdir/analyzed/'
        base_path = os.path.join(workdir, 'analyzed')
        print(f"Downloading GTFS files from {base_url} to {base_path}")
        for filename in ['route_stats.csv.gz', 'trip_stats.csv.gz']:
            url = base_url + filename
            path = os.path.join(base_path, filename)
            common.http_stream_download(path, url=url)
