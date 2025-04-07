import dagster
import os
from dagster import Definitions, load_assets_from_modules
# Imports
from pipeline.assets.ingestion import partitioned_fast_open_assets, single_file_open_assets
from pipeline.resources.io_managers.fastopendata_single_io_manager import FastOpenDataSingleFileParquetIoManager
from pipeline.resources.io_managers.fastopendata_partitioned_parquet_io_manager import FastOpenDataPartitionedParquetIOManager
from pipeline.constants import LAKE_PATH

single_file_open_assets  = load_assets_from_modules([single_file_open_assets])

partitioned_fast_open_assets = load_assets_from_modules([partitioned_fast_open_assets])

# Partition-based IO manager (existing)
fastopendata_partitioned_parquet_io_manager = FastOpenDataPartitionedParquetIOManager(
    base_dir=LAKE_PATH
)

# Single-file IO manager (new)
fastopendata_singlefile_io_manager = FastOpenDataSingleFileParquetIoManager(
    base_dir=LAKE_PATH
)

# Combine both managers in resources
resources = {
    "fastopendata_partitioned_parquet_io_manager": fastopendata_partitioned_parquet_io_manager,           # for partition-based assets
    "fastopendata_singlefile_io_manager": fastopendata_singlefile_io_manager,  # for single-file assets
}

# Combine both sets of assets
all_assets = partitioned_fast_open_assets + single_file_open_assets

defs = Definitions(
    assets=all_assets,
    resources=resources
)