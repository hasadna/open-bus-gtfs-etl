#!/usr/bin/env python3

import datetime
import os
from pathlib import Path
from typing import Tuple, Optional

from pandas import DataFrame

from src.gtfs_stat.core_computations import get_zones_df, get_clusters_df, get_trip_id_to_date_df, compute_trip_stats, \
    compute_route_stats
from src.gtfs_stat.output import save_dataframe_to_file
from src.gtfs_stat.partridge_helper import prepare_partridge_feed


def analyze_gtfs_date(date: datetime.date, gtfs_file_path: Path, tariff_file_path: Path,
                      cluster_to_line_file_path: Path, trip_id_to_date_file_path: Path) -> Tuple[DataFrame, DataFrame]:
    """
    Handles analysis of a single date for GTFS. Computes and saves stats files (currently trip_stats
    and route_stats).
    """

    feed = prepare_partridge_feed(date, gtfs_file_path)

    zones = get_zones_df(tariff_file_path)

    clusters = get_clusters_df(cluster_to_line_file_path)

    trip_id_to_date_df = get_trip_id_to_date_df(trip_id_to_date_file_path, date)

    ts = compute_trip_stats(feed, zones, clusters, trip_id_to_date_df, date)
    rs = compute_route_stats(ts, date)

    return ts, rs


def _dump_trip_and_route_stat(trip_stat: DataFrame, route_stat: DataFrame, output_folder: Path):
    os.makedirs(output_folder, exist_ok=True)
    save_dataframe_to_file(trip_stat, output_folder.joinpath('trip_stats.csv.gz'))
    save_dataframe_to_file(route_stat, output_folder.joinpath('route_stats.csv.gz'))


def create_trip_and_route_stat(date_to_analyze: datetime.date, gtfs_file_path: Path, tariff_file_path: Path,
                               cluster_to_line_file_path: Path, trip_id_to_date_file_path: Path,
                               output_folder: Optional[Path] = None) -> Tuple[DataFrame, DataFrame]:

    ts, rs = analyze_gtfs_date(date=date_to_analyze, gtfs_file_path=gtfs_file_path, tariff_file_path=tariff_file_path,
                               cluster_to_line_file_path=cluster_to_line_file_path,
                               trip_id_to_date_file_path=trip_id_to_date_file_path)

    if output_folder is not None:
        _dump_trip_and_route_stat(ts, rs, output_folder)

    return ts, rs
