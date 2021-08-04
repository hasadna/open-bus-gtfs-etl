import datetime
from pathlib import Path

from open_bus_gtfs_etl.gtfs_extractor.gtfs_extractor import GtfsRetriever, GTFSFiles
from open_bus_gtfs_etl.gtfs_loader import load_routes_to_db
from open_bus_gtfs_etl.gtfs_stat.gtfs_stats import create_trip_and_route_stat, read_stat_file, dump_trip_and_route_stat
import click


@click.command()
@click.option('--date-to-analyze', default=None)
@click.option('--outputs-folder', default=None)
@click.option('--gtfs', default=None)
@click.option('--tariff', default=None)
@click.option('--cluster-to-line', default=None)
@click.option('--trip-id-to-date', default=None)
@click.option('--route-stat-file', default=None)
def basic_command(date_to_analyze: datetime.date = None, outputs_folder: Path = None, gtfs: Path = None,
                  tariff: Path = None, cluster_to_line: Path = None, trip_id_to_date: Path = None,
                  route_stat_file: Path = None):
    gtfs_files: GTFSFiles = GTFSFiles(gtfs=gtfs, tariff=tariff, cluster_to_line=cluster_to_line,
                                      trip_id_to_date=trip_id_to_date)

    main(date_to_analyze, outputs_folder, gtfs_files, route_stat_file)


def main(date_to_analyze: datetime.date = None, outputs_folder: Path = None, gtfs_files: GTFSFiles = None,
         route_stat_file: Path = None):
    """

    :param outputs_folder: folder to save the outputs of the process
    :param gtfs_files: location of the gtfs - if not provided will download files from MOT FTP
    :param date_to_analyze: gtfs file will be filtered by that date - default is today
    :param route_stat_file: in case route stat file is provided extract and transformation steps
     are skipped

    :return:
    """

    if date_to_analyze is None:
        date_to_analyze = datetime.datetime.now().date()

    if route_stat_file is None:

        if gtfs_files is None:
            gtfs_files: GTFSFiles = GtfsRetriever(outputs_folder).retrieve_gtfs_files()

        trip_stat, route_stat = create_trip_and_route_stat(
            date_to_analyze=date_to_analyze, gtfs_file_path=gtfs_files.gtfs,
            tariff_file_path=gtfs_files.tariff, output_folder=outputs_folder,
            trip_id_to_date_file_path=gtfs_files.trip_id_to_date,
            cluster_to_line_file_path=gtfs_files.cluster_to_line)

        if outputs_folder is not None:
            dump_trip_and_route_stat(trip_stat, route_stat, outputs_folder)

    else:
        route_stat = read_stat_file(route_stat_file)

    load_routes_to_db(route_stat=route_stat)

if __name__ == '__main__':
    basic_command()