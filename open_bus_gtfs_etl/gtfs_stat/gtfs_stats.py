from datetime import date
import os
from pathlib import Path
from typing import Tuple

from pandas import DataFrame

from open_bus_gtfs_etl.gtfs_extractor.gtfs_extractor import GTFSFiles
from open_bus_gtfs_etl.gtfs_stat.core_computations import get_zones_df, get_clusters_df, get_trip_id_to_date_df, \
    compute_trip_stats, compute_route_stats
from open_bus_gtfs_etl.gtfs_stat.output import save_dataframe_to_file
from open_bus_gtfs_etl.gtfs_stat.partridge_helper import prepare_partridge_feed

TRIP_STAT_FILE_NAME = 'trip_stats.csv.gz'
ROUTE_STAT_FILE_NAME = 'route_stats.csv.gz'


def analyze_gtfs_date(date_to_analyze: date, gtfs_file_path: Path, tariff_file_path: Path,
                      cluster_to_line_file_path: Path, trip_id_to_date_file_path: Path) -> Tuple[DataFrame, DataFrame]:
    """
    Aggregate GTFS data of single date into trip-stat and route stat DataFrames
    """

    feed = prepare_partridge_feed(date_to_analyze, gtfs_file_path)

    zones = get_zones_df(tariff_file_path)

    clusters = get_clusters_df(cluster_to_line_file_path)

    trip_id_to_date_df = get_trip_id_to_date_df(trip_id_to_date_file_path, date_to_analyze)

    trip_stats = compute_trip_stats(feed, zones, clusters, trip_id_to_date_df, date_to_analyze)
    route_stats = compute_route_stats(trip_stats, date_to_analyze)

    return trip_stats, route_stats


def dump_trip_and_route_stat(trip_stat: DataFrame, route_stat: DataFrame, output_folder: Path) -> Path:
    os.makedirs(output_folder, exist_ok=True)
    path_of_routes_stat = output_folder.joinpath(TRIP_STAT_FILE_NAME)
    save_dataframe_to_file(trip_stat, path_of_routes_stat)
    save_dataframe_to_file(route_stat, output_folder.joinpath(ROUTE_STAT_FILE_NAME))
    return path_of_routes_stat


def create_trip_and_route_stat(date_to_analyze: date, gtfs_files: GTFSFiles) \
        -> Tuple[DataFrame, DataFrame]:
    """
    trip_stat, route_stat = create_trip_and_route_stat()
    """

    trip_stats, route_stats = analyze_gtfs_date(
        date_to_analyze=date_to_analyze, gtfs_file_path=gtfs_files.gtfs.absolute(),
        tariff_file_path=gtfs_files.tariff.absolute(), cluster_to_line_file_path=gtfs_files.cluster_to_line.absolute(),
        trip_id_to_date_file_path=gtfs_files.trip_id_to_date.absolute())

    return trip_stats, route_stats
