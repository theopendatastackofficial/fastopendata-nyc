import dagster
import os
from dagster import Definitions, load_assets_from_modules
# Existing imports
from pipeline.assets.ingestion import partitioned_fast_open_assets
from pipeline.resources.io_managers.fastopendata_partitioned_parquet_io_manager import FastOpenDataPartitionedParquetIOManager
from pipeline.constants import LAKE_PATH

# Add the new imports
from pipeline.assets.ingestion import single_file_open_assets
from pipeline.resources.io_managers.fastopendata_single_file_parquet_io_manager import FastOpenDataSingleFileParquetIoManager

# Load existing "fast open" assets
partitioned_fast_open_assets = load_assets_from_modules([partitioned_fast_open_assets])

# Load our new single-file assets
single_file_assets = load_assets_from_modules([single_file_open_assets])

# Create the existing partition-based IO manager
fastopendata_partitioned_parquet_io_manager = FastOpenDataPartitionedParquetIOManager(
    base_dir=LAKE_PATH  
)

# Create the new single-file IO manager
fastopendata_singlefile__parquet_io_manager = FastOpenDataSingleFileParquetIoManager(
    base_dir=LAKE_PATH
)

# Bundle both IO managers into the resources dictionary
resources = {
    "fastopendata_partitioned_parquet_io_manager": fastopendata_partitioned_parquet_io_manager,
    "fastopendata_singlefile__parquet_io_manager": fastopendata_singlefile__parquet_io_manager,
}

# Combine both sets of assets into a single Definitions object
defs = Definitions(
    assets=partitioned_fast_open_assets + single_file_assets,
    resources=resources
)
