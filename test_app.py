import datetime
import os
from pathlib import Path

from gtfs_stat.gtfs_stats import create_trip_and_route_stat


class TestGtgsStat:
    def test_sample_run(self, tmp_path: Path):

        base = "resources/gtfs_stat_assets"
        create_trip_and_route_stat(date_to_analyze=datetime.date(2019, 3, 7),
                                   gtfs_file_name=os.path.join(base, "2019-03-07-israel-public-transportation.zip"),
                                   tariff_zip_name=os.path.join(base, "2019-03-07-Tariff.zip"),
                                   cluster_to_line_zip_name=os.path.join(base, "2019-03-07-ClusterToLine.zip"),
                                   trip_id_to_date_zip_name=os.path.join(base, "2019-03-07-TripIdToDate.zip"),
                                   output_folder=tmp_path.as_posix())

        assert tmp_path.joinpath('route_stats.csv.gz').is_file()
        assert tmp_path.joinpath('trip_stats.csv.gz').is_file()



