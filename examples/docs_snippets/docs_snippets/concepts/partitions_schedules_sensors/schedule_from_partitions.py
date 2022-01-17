"""isort:skip_file"""

# start_marker
from dagster import build_schedule_from_partitioned_job, job


@job
def do_stuff_partitioned():
    ...


do_stuff_partitioned_schedule = build_schedule_from_partitioned_job(
    do_stuff_partitioned,
)

# end_marker
