from . import common
from textwrap import dedent


def main(dates):
    dates_where = []
    for date in dates:
        dates_where.append(f"'{common.parse_date_str(date)}'")
    dates_where = 'date in (' + ', '.join(dates_where) + ')'
    print(dedent(f"""
-- Before attempting this you should stop the airflow processing:
-- Disable autosync for the openbus application in the argocd app configuration yaml
-- Scale the airflow scheduler deployment to 0
-- Wait for the airflow scheduler pod to be terminated

-- Following SQL deletes all gtfs date details
-- note that this is highly dependant on the current state of the code and it might not work in the future
-- Last updated: May 9, 2023

alter table siri_ride disable TRIGGER all;

update siri_ride
set route_gtfs_ride_id = null, scheduled_time_gtfs_ride_id = null, journey_gtfs_ride_id = null, gtfs_ride_id = null
from gtfs_ride, gtfs_route
where gtfs_route.{dates_where}
and gtfs_route.id = gtfs_ride.gtfs_route_id
and gtfs_ride.id = siri_ride.route_gtfs_ride_id;

update siri_ride
set route_gtfs_ride_id = null, scheduled_time_gtfs_ride_id = null, journey_gtfs_ride_id = null, gtfs_ride_id = null
from gtfs_ride, gtfs_route
where gtfs_route.{dates_where}
and gtfs_route.id = gtfs_ride.gtfs_route_id
and gtfs_ride.id = siri_ride.gtfs_ride_id;

update siri_ride
set route_gtfs_ride_id = null, scheduled_time_gtfs_ride_id = null, journey_gtfs_ride_id = null, gtfs_ride_id = null
from gtfs_ride, gtfs_route
where gtfs_route.{dates_where}
and gtfs_route.id = gtfs_ride.gtfs_route_id
and gtfs_ride.id = siri_ride.journey_gtfs_ride_id;

update siri_ride
set route_gtfs_ride_id = null, scheduled_time_gtfs_ride_id = null, journey_gtfs_ride_id = null, gtfs_ride_id = null
from gtfs_ride, gtfs_route
where gtfs_route.{dates_where}
and gtfs_route.id = gtfs_ride.gtfs_route_id
and gtfs_ride.id = siri_ride.scheduled_time_gtfs_ride_id;

alter table siri_ride enable TRIGGER all;

update siri_ride_stop set gtfs_stop_id = null
from gtfs_stop
where gtfs_stop.{dates_where}
and gtfs_stop.id = siri_ride_stop.gtfs_stop_id;

DROP INDEX public.ix_gtfs_ride_gtfs_route_id;
DROP INDEX public.ix_gtfs_ride_journey_ref;
DROP INDEX public.ix_gtfs_ride_start_time;
alter table gtfs_ride drop constraint fk_gtfs_ride_first_gtfs_ride_stop_id_gtfs_ride_stop;
alter table gtfs_ride drop constraint fk_gtfs_ride_last_gtfs_ride_stop_id_gtfs_ride_stop;
alter table gtfs_ride disable TRIGGER all;

delete from gtfs_ride_stop
using gtfs_ride, gtfs_route
where gtfs_route.{dates_where}
and gtfs_route.id = gtfs_ride.gtfs_route_id
and gtfs_ride.id = gtfs_ride_stop.gtfs_ride_id;

delete from gtfs_ride_stop
using gtfs_stop
where gtfs_stop.{dates_where}
and gtfs_stop.id = gtfs_ride_stop.gtfs_stop_id;

DELETE FROM gtfs_ride
USING gtfs_route
WHERE gtfs_ride.gtfs_route_id = gtfs_route.id
AND gtfs_route.{dates_where};

alter table gtfs_ride enable TRIGGER all;
alter table public.gtfs_ride add constraint fk_gtfs_ride_first_gtfs_ride_stop_id_gtfs_ride_stop
    foreign key (first_gtfs_ride_stop_id) references public.gtfs_ride_stop;
alter table public.gtfs_ride add constraint fk_gtfs_ride_last_gtfs_ride_stop_id_gtfs_ride_stop
    foreign key (last_gtfs_ride_stop_id) references public.gtfs_ride_stop;
CREATE INDEX ix_gtfs_ride_gtfs_route_id ON public.gtfs_ride (gtfs_route_id);
CREATE INDEX ix_gtfs_ride_journey_ref ON public.gtfs_ride (journey_ref);
CREATE INDEX ix_gtfs_ride_start_time ON public.gtfs_ride (start_time);

delete from gtfs_route
where {dates_where};

delete from gtfs_stop_mot_id
using gtfs_stop
where gtfs_stop_mot_id.id = gtfs_stop.id
and gtfs_stop.{dates_where};

delete from gtfs_stop
where {dates_where};

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

-- The hourly gtfs processing task should now be able to process the data
    """))
