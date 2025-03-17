import os
import gc
import requests
import polars as pl
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dagster import asset, OpExecutionContext, Output

@asset(
    name="mta_subway_hourly_ridership",
    io_manager_key="fastopendata_partitioned_parquet_io_manager",
)
def mta_subway_hourly_ridership():
    """
    Asset that wants data from March 2022 to December 2024, for example.
    Instead of returning a Polars DataFrame, we return a dict with the
    start/end that the IO manager can use to fetch data from R2.
    """
    return {
        "start_year": 2022,
        "start_month": 3,
        "end_year": 2024,
        "end_month": 12
    }



@asset(
    name="mta_subway_origin_destination_2023",
    io_manager_key="fastopendata_partitioned_parquet_io_manager",
)
def mta_subway_origin_destination_2023():
    """
    Asset that wants data from March 2022 to December 2024, for example.
    Instead of returning a Polars DataFrame, we return a dict with the
    start/end that the IO manager can use to fetch data from R2.
    """
    return {
        "start_year": 2023,
        "start_month": 1,
        "end_year": 2023,
        "end_month": 12
    }



@asset(
    name="mta_subway_origin_destination_2024",
    io_manager_key="fastopendata_partitioned_parquet_io_manager",
)
def mta_subway_origin_destination_2024():
    """
    Asset that wants data from March 2022 to December 2024, for example.
    Instead of returning a Polars DataFrame, we return a dict with the
    start/end that the IO manager can use to fetch data from R2.
    """
    return {
        "start_year": 2024,
        "start_month": 1,
        "end_year": 2024,
        "end_month": 12
    }


@asset(
    name="nyc_threeoneone_requests",
    io_manager_key="fastopendata_partitioned_parquet_io_manager",
)
def nyc_threeoneone_requests():
    """
    Asset that wants data from January 2010 up to January 2025, for example.
    """
    return {
        "start_year": 2010,
        "start_month": 1,
        "end_year": 2025,
        "end_month": 1
    }