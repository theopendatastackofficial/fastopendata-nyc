import dagster
import os
from dagster import Definitions, load_assets_from_modules
from pipeline.assets.ingestion import fast_open_assets #Our ingestion assets

from pipeline.constants import LAKE_PATH  #The base path for our data lake of parquet files

from pipeline.resources.io_managers.fastopendata_io_manager import FastOpenDataParquetIOManager #Our IO Manager for storing dagster dataframes as parquet files

# Load MTA and Weather assets
fast_open_assets = load_assets_from_modules([fast_open_assets])


fastopendata_io_manager = FastOpenDataParquetIOManager(
    base_dir=LAKE_PATH  
)

# Then, bundle all of them into resources
resources = {
    "fastopendata_io_manager": fastopendata_io_manager,  
}


# Define the Dagster assets taking part in our data platform, and the resources they can use
defs = Definitions(
    assets=fast_open_assets,
    resources=resources
)