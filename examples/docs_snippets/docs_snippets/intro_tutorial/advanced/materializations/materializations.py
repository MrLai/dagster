import csv
import os

import requests
from dagster import (
    AssetMaterialization,
    EventMetadata,
    Output,
    get_dagster_logger,
    job,
    op,
)


@op
def download_csv():
    response = requests.get("https://docs.dagster.io/assets/cereal.csv")
    lines = response.text.split("\n")
    get_dagster_logger().info(f"Read {len(lines)} lines")
    return [row for row in csv.DictReader(lines)]


# start_materializations_marker_0
@op
def sort_by_calories(context, cereals):
    sorted_cereals = sorted(
        cereals, key=lambda cereal: int(cereal["calories"])
    )
    least_caloric = sorted_cereals[0]["name"]
    most_caloric = sorted_cereals[-1]["name"]

    logger = get_dagster_logger()
    logger.info(f"Least caloric cereal: {least_caloric}")
    logger.info(f"Most caloric cereal: {most_caloric}")

    fieldnames = list(sorted_cereals[0].keys())
    sorted_cereals_csv_path = os.path.abspath(
        f"output/calories_sorted_{context.run_id}.csv"
    )
    os.makedirs(os.path.dirname(sorted_cereals_csv_path), exist_ok=True)

    with open(sorted_cereals_csv_path, "w") as fd:
        writer = csv.DictWriter(fd, fieldnames)
        writer.writeheader()
        writer.writerows(sorted_cereals)

    yield AssetMaterialization(
        asset_key="sorted_cereals_csv",
        description="Cereals data frame sorted by caloric content",
        metadata={
            "sorted_cereals_csv_path": EventMetadata.path(
                sorted_cereals_csv_path
            )
        },
    )
    yield Output(None)


# end_materializations_marker_0


@job
def materialization_job():
    sort_by_calories(download_csv())
