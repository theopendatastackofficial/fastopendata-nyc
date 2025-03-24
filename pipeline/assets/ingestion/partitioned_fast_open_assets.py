import os
import gc
import requests
import polars as pl
from datetime import datetime
from dagster import asset, Output, MetadataValue

@asset(
    name="mta_subway_hourly_ridership",
    io_manager_key="fastopendata_partitioned_parquet_io_manager",
    group_name="311",
    tags={"domain": "MTA", "type": "ingestion", "source": "fastopendata"},
    metadata={
        "data_url": MetadataValue.url("https://data.ny.gov/Transportation/MTA-Subway-Hourly-Ridership-2020-2024/wujg-7c2s/about_data")
    }
)
def mta_subway_hourly_ridership():
    return {
        "start_year": 2022,
        "start_month": 3,
        "end_year": 2024,
        "end_month": 12
    }

@asset(
    name="mta_subway_origin_destination_2023",
    io_manager_key="fastopendata_partitioned_parquet_io_manager",
    group_name="MTA",
    tags={"domain": "MTA", "type": "ingestion", "source": "fastopendata"},
    metadata={
        "data_url": MetadataValue.url("https://data.ny.gov/Transportation/MTA-Subway-Origin-Destination-Ridership-Estimate-2/uhf3-t34z/about_data")
    }
)
def mta_subway_origin_destination_2023():
    return {
        "start_year": 2023,
        "start_month": 1,
        "end_year": 2023,
        "end_month": 12
    }

@asset(
    name="mta_subway_origin_destination_2024",
    io_manager_key="fastopendata_partitioned_parquet_io_manager",
    group_name="311",
    tags={"domain": "crime", "type": "ingestion", "source": "fastopendata"},
    metadata={
        "data_url": MetadataValue.url("https://data.ny.gov/Transportation/MTA-Subway-Origin-Destination-Ridership-Estimate-2/jsu2-fbtj/about_data")
    }
)
def mta_subway_origin_destination_2024():
    return {
        "start_year": 2024,
        "start_month": 1,
        "end_year": 2024,
        "end_month": 12
    }

@asset(
    name="nyc_threeoneone_requests",
    io_manager_key="fastopendata_partitioned_parquet_io_manager",
    group_name="311",
    tags={"domain": "crime", "type": "ingestion", "source": "fastopendata"},
    metadata={
        "data_url": MetadataValue.url("https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9/about_data")
    }
)
def nyc_threeoneone_requests():
    return {
        "start_year": 2010,
        "start_month": 1,
        "end_year": 2025,
        "end_month": 1
    }

@asset(
    name="crime_nypd_arrests",
    io_manager_key="fastopendata_partitioned_parquet_io_manager",
    group_name="crime",
    tags={"domain": "crime", "type": "ingestion", "source": "fastopendata"},
    metadata={
        "data_url": MetadataValue.url("https://data.cityofnewyork.us/Public-Safety/NYPD-Arrests-Data-Historic-/8h9b-rp9u/about_data")
    }
)
def crime_nypd_arrests():
    return {
        "start_year": 2013,
        "start_month": 3,
        "end_year": 2023,
        "end_month": 12
    }