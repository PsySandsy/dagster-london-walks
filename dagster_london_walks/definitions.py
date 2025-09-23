from dagster import Definitions, load_assets_from_modules
from dagster_aws.s3 import S3Resource

from dagster_london_walks import assets  # noqa: TID252

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets
    # resources={"s3": S3Resource(region_name="eu-west-2")}
)
