import datetime
from functools import lru_cache

import numpy as np
import partridge as ptg


def get_partridge_filter_for_date(zip_path: str, date: datetime.date):
    service_ids = ptg.read_service_ids_by_date(zip_path)[date]

    return {
        'trips.txt': {
            'service_id': service_ids,
        },
    }


def get_partridge_feed_by_date(zip_path: str, date: datetime.date):
    return ptg.feed(zip_path, view=get_partridge_filter_for_date(zip_path, date))


def prepare_partridge_feed(date: datetime.date, gtfs_file_full_path: str):
    return get_partridge_feed_by_date(gtfs_file_full_path, date)


# Copied from partridge parsers, with a deletion of the seconds field
# it is used to to parse TripIdToDate since the departure time is in HH:MM format
# Why 2^17? See https://git.io/vxB2P.
@lru_cache(maxsize=2 ** 17)
def parse_time_no_seconds(val: str) -> np.float64:
    if val is np.nan:
        return val

    val = val.strip()

    if val == "":
        return np.nan

    h, m = val.split(":")
    ssm = int(h) * 3600 + int(m) * 60

    # pandas doesn't have a NaN int, use floats
    return np.float64(ssm)


parse_time_no_seconds_column = np.vectorize(parse_time_no_seconds)
