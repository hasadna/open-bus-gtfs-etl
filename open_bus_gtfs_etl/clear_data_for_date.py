from . import common
from textwrap import dedent


def main(dates):
    dates_where = []
    date_commands = []
    for date in dates:
        dates_where.append(f"'{common.parse_date_str(date)}'")
        date_commands.append(f'-- open-bus-gtfs-etl idempotent-process --only-date {date}')
    dates_where = 'date in (' + ', '.join(dates_where) + ')'
    date_commands = '\n'.join(date_commands)
    print(dedent(f"""
    
-- Delete all gtfs date details, note that this is highly dependant on the current state of the code and it might not work in the future
-- Last updated: May 9, 2023

update siri_ride set route_gtfs_ride_id = null, scheduled_time_gtfs_ride_id = null, journey_gtfs_ride_id = null, gtfs_ride_id = null
where route_gtfs_ride_id in (select id from gtfs_ride where gtfs_route_id in (select id from gtfs_route where {dates_where}))
or  scheduled_time_gtfs_ride_id in (select id from gtfs_ride where gtfs_route_id in (select id from gtfs_route where {dates_where}))
or  journey_gtfs_ride_id in (select id from gtfs_ride where gtfs_route_id in (select id from gtfs_route where {dates_where}))
or  gtfs_ride_id in (select id from gtfs_ride where gtfs_route_id in (select id from gtfs_route where {dates_where}));

update siri_ride_stop set gtfs_stop_id = null where gtfs_stop_id in (
    select id from gtfs_stop where {dates_where}
);

DROP INDEX public.ix_gtfs_ride_gtfs_route_id;
DROP INDEX public.ix_gtfs_ride_journey_ref;
DROP INDEX public.ix_gtfs_ride_start_time;

delete from gtfs_ride_stop where gtfs_ride_id in (
    select id from gtfs_ride where gtfs_route_id in (
        select id from gtfs_route where {dates_where}
    )
) or gtfs_stop_id in (
    select id from gtfs_stop where {dates_where}
);

DELETE FROM gtfs_ride
USING gtfs_route
WHERE gtfs_ride.gtfs_route_id = gtfs_route.id
AND gtfs_route.{dates_where};

CREATE INDEX ix_gtfs_ride_gtfs_route_id ON public.gtfs_ride (gtfs_route_id);
CREATE INDEX ix_gtfs_ride_journey_ref ON public.gtfs_ride (journey_ref);
CREATE INDEX ix_gtfs_ride_start_time ON public.gtfs_ride (start_time);

delete from gtfs_route where {dates_where};

delete from gtfs_stop_mot_id where gtfs_stop_id in (
    select id from gtfs_stop where {dates_where}
);

delete from gtfs_stop where {dates_where};

update gtfs_data 
set
    processing_started_at = null,
    processing_completed_at = null,
    processing_error = null,
    processing_success = null,
    processing_used_stride_date = null,
    download_upload_started_at = null,
    download_upload_completed_at = null,
    download_upload_error = null,
    download_upload_success = true
where {dates_where};

-- To recreate the data - exec shell on airflow-scheduler pod and run the following:
-- source $STRIDE_VENV/bin/activate
{date_commands}

    """))
