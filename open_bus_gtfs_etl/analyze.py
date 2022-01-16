import os
from pathlib import Path

from . import common, config
from .gtfs_stat import partridge_helper
from open_bus_gtfs_etl.gtfs_stat import core_computations
from open_bus_gtfs_etl.gtfs_stat import gtfs_stats


def main(date: str, workdir: str):
    date = common.parse_date_str(date)
    workdir = common.get_workdir(workdir)
    output_folder = os.path.join(workdir, config.WORKDIR_ANALYZED_OUTPUT)
    print("analyzing GTFS files for date {} from workdir {}, saving output to {}".format(
        date, workdir, output_folder))
    with common.print_memory_usage("Preparing partridge feed..."):
        feed = partridge_helper.prepare_partridge_feed(
            date, Path(workdir, config.WORKDIR_ISRAEL_PUBLIC_TRANSPORTATION)
        )
    with common.print_memory_usage("Getting zones df..."):
        zones = core_computations.get_zones_df(
            os.path.join(workdir, config.WORKDIR_TARIFF)
        )
    with common.print_memory_usage("Getting clusters df..."):
        clusters = core_computations.get_clusters_df(
            os.path.join(workdir, config.WORKDIR_CLUSTER_TO_LINE)
        )
    with common.print_memory_usage("Getting trip id to date df..."):
        trip_id_to_date_df = core_computations.get_trip_id_to_date_df(
            Path(workdir, config.WORKDIR_TRIP_ID_TO_DATE),
            date
        )
    with common.print_memory_usage('computing trip stats...'):
        trip_stats = core_computations.compute_trip_stats(
            feed, zones, clusters, trip_id_to_date_df, date
        )
    with common.print_memory_usage('computing route stats...'):
        route_stats = core_computations.compute_route_stats(trip_stats, date)
    with common.print_memory_usage('dumping trip and route stat...'):
        gtfs_stats.dump_trip_and_route_stat(
            trip_stat=trip_stats, route_stat=route_stats,
            output_folder=Path(output_folder)
        )
