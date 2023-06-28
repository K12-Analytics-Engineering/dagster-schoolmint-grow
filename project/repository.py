import os

from assets.schoolmint_grow import create_schoolmint_grow_asset
from io_managers.gcs_io_manager import gcs_io_manager
from resources.schoolmint_grow_api_resource import SchoolMintGrowApiClient
from dagster import (
    AssetSelection,
    define_asset_job,
    Definitions,
    fs_io_manager,
    multiprocess_executor,
    EnvVar,
    ScheduleDefinition,
)
from dagster_gcp.gcs.io_manager import gcs_pickle_io_manager
from dagster_gcp.gcs.resources import gcs_resource


RESOURCES_LOCAL = {
    "gcs": gcs_resource,
    "io_manager": fs_io_manager,
    "gcs_io_manager": gcs_io_manager.configured(
        {
            "gcs_bucket_name": "",
        }
    ),
    "schoolmint_grow_client": SchoolMintGrowApiClient(
        api_key=EnvVar("SCHOOLMINT_GROW_API_KEY"),
        api_secret=EnvVar("SCHOOLMINT_GROW_API_SECRET"),
    ),
}

RESOURCES_PROD = {
    "gcs": gcs_resource,
    "io_manager": gcs_pickle_io_manager.configured(
        {"gcs_bucket": "", "gcs_prefix": "dagster_io"}
    ),
    "gcs_io_manager": gcs_io_manager.configured(
        {
            "gcs_bucket_name": "",
        }
    ),
    "schoolmint_grow_client": SchoolMintGrowApiClient(
        api_key=EnvVar("SCHOOLMINT_GROW_API_KEY"),
        api_secret=EnvVar("SCHOOLMINT_GROW_API_SECRET"),
    ),
}

resource_defs_by_deployment_name = {
    "prod": RESOURCES_PROD,
    "local": RESOURCES_LOCAL,
}


defs = Definitions(
    assets=[
        create_schoolmint_grow_asset(
            "base_schoolmint_grow_measurements",
            "measurements",
        ),
        create_schoolmint_grow_asset(
            "base_schoolmint_grow_meetings",
            "meetings",
        ),
        create_schoolmint_grow_asset(
            "base_schoolmint_grow_observations",
            "observations",
        ),
        create_schoolmint_grow_asset(
            "base_schoolmint_grow_rubrics",
            "rubrics",
        ),
        create_schoolmint_grow_asset(
            "base_schoolmint_grow_schools",
            "schools",
        ),
        create_schoolmint_grow_asset(
            "base_schoolmint_grow_users",
            "users",
        ),
    ],
    schedules=[
        ScheduleDefinition(
            job=define_asset_job("assets_job", selection=AssetSelection.all()),
            cron_schedule="0 0 * * *",
            execution_timezone="America/Chicago",
        ),
    ],
    sensors=[],
    jobs=None,
    resources=resource_defs_by_deployment_name[
        os.environ.get("DAGSTER_DEPLOYMENT", "local")
    ],
    executor=multiprocess_executor.configured({"max_concurrent": 2}),
)
