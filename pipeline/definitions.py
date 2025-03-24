import dagster
import os
from dagster import Definitions, load_assets_from_modules
# Imports
from pipeline.assets.ingestion import partitioned_fast_open_assets
from pipeline.resources.io_managers.fastopendata_partitioned_parquet_io_manager import FastOpenDataPartitionedParquetIOManager
from pipeline.constants import LAKE_PATH

# Load existing "fast open" assets
partitioned_fast_open_assets = load_assets_from_modules([partitioned_fast_open_assets])


# Create the existing partition-based IO manager
fastopendata_partitioned_parquet_io_manager = FastOpenDataPartitionedParquetIOManager(
    base_dir=LAKE_PATH  
)

# IO manager for downloading from Fast Open Data into the resources dictionary
resources = {
    "fastopendata_partitioned_parquet_io_manager": fastopendata_partitioned_parquet_io_manager,
}

# Combine both sets of assets into a single Definitions object
defs = Definitions(
    assets=partitioned_fast_open_assets,
    resources=resources
)
