#For filesystem operations
import os


# Define the base path relative to the location where we will keep our data lake of parquet files. Our lake base path is one folder back, then data/opendata
LAKE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "opendata"))

# Path to where we will store our Dagster logs
DAGSTER_PATH=os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "logs"))

# Base path to store our SQLite file, powers our local data dictionary application
SQLITE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "metadata", "metadata.db")) #

