#!/usr/bin/env python3

import datetime
import os
from typing import Tuple, Optional

from pandas import DataFrame

from gtfs_stat.core_computations import get_zones_df, get_clusters_df, get_trip_id_to_date_df, compute_trip_stats, \
    compute_route_stats
from gtfs_stat.output import save_dataframe_to_file
from gtfs_stat.partridge_helper import prepare_partridge_feed


def analyze_gtfs_date(date: datetime.date, gtfs_file_name, tariff_zip_name, cluster_to_line_zip_name,
                      trip_id_to_date_zip_name) -> Tuple[DataFrame, DataFrame]:
    """
    Handles analysis of a single date for GTFS. Computes and saves stats files (currently trip_stats
    and route_stats).
    """

    feed = prepare_partridge_feed(date, gtfs_file_name)

    zones = get_zones_df(tariff_zip_name)

    clusters = get_clusters_df(cluster_to_line_zip_name)

    trip_id_to_date_df = get_trip_id_to_date_df(trip_id_to_date_zip_name, date)

    ts = compute_trip_stats(feed, zones, clusters, trip_id_to_date_df, date)
    rs = compute_route_stats(ts, date)

    return ts, rs


def _dump_trip_and_route_stat(trip_stat: DataFrame, route_stat: DataFrame, output_folder: str):
    os.makedirs(output_folder, exist_ok=True)
    save_dataframe_to_file(trip_stat, os.path.join(output_folder, 'trip_stats.csv.gz'))
    save_dataframe_to_file(route_stat, os.path.join(output_folder, 'route_stats.csv.gz'))


def create_trip_and_route_stat(date_to_analyze: datetime.date, gtfs_file_name: str, tariff_zip_name: str,
                               cluster_to_line_zip_name: str, trip_id_to_date_zip_name: str,
                               output_folder: Optional[str] = None) -> Tuple[DataFrame, DataFrame]:

    ts, rs = analyze_gtfs_date(date=date_to_analyze, gtfs_file_name=gtfs_file_name, tariff_zip_name=tariff_zip_name,
                               cluster_to_line_zip_name=cluster_to_line_zip_name,
                               trip_id_to_date_zip_name=trip_id_to_date_zip_name)

    if output_folder is not None:
        _dump_trip_and_route_stat(ts, rs, output_folder)

    return ts, rs
