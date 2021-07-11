import datetime
import tempfile
from pathlib import Path

from src.gtfs_extractor.gtfs_extractor import GtfsRetriever, GTFSFiles
from src.gtfs_stat.gtfs_stats import create_trip_and_route_stat


def main(outputs_folder: Path, gtfs_files: GTFSFiles = None, date_to_analyze: datetime.date = None):
    """

    :param outputs_folder: folder to save the outputs of the process
    :param gtfs_files: location of the gtfs - if not provided will download files from MOT FTP
    :param date_to_analyze: gtfs file will be filtered by that date - default is today
    :return:
    """

    if gtfs_files is None:
        gtfs_files: GTFSFiles = GtfsRetriever(outputs_folder).retrieve_gtfs_files()

    if date_to_analyze is None:
        date_to_analyze = datetime.datetime.now().date()

    create_trip_and_route_stat(date_to_analyze=date_to_analyze,
                               gtfs_file_path=gtfs_files.gtfs, tariff_file_path=gtfs_files.tariff,
                               cluster_to_line_file_path=gtfs_files.cluster_to_line,
                               trip_id_to_date_file_path=gtfs_files.trip_id_to_date,
                               output_folder=outputs_folder)



if __name__ == '__main__':
    with tempfile.TemporaryDirectory() as tmpdirname:
        main(outputs_folder=tmpdirname)
        print("The End")

