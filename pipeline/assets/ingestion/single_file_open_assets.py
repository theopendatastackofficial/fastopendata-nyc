from dagster import asset

@asset(
    name="nyc_dob_certificate_occupancy",
    io_manager_key="fastopendata_singlefile__parquet_io_manager",
)
def nyc_dob_certificate_occupancy():
    """
    Single-file asset for NYC DOB Certificate of Occupancy.
    Materializing this asset triggers the IO manager to download:
    LAKE_PATH/nyc_dob_certificate_occupancy/nyc_dob_certificate_occupancy.parquet
    """
    return None

@asset(
    name="nyc_dob_now_certificate_occupancy",
    io_manager_key="fastopendata_singlefile__parquet_io_manager",
)
def nyc_dob_now_certificate_occupancy():
    return None

@asset(
    name="nyc_dob_safety_violations",
    io_manager_key="fastopendata_singlefile__parquet_io_manager",
)
def nyc_dob_safety_violations():
    return None

@asset(
    name="nyc_dof_building_sales",
    io_manager_key="fastopendata_singlefile__parquet_io_manager",
)
def nyc_dof_building_sales():
    return None

@asset(
    name="nyc_hpd_aep",
    io_manager_key="fastopendata_singlefile__parquet_io_manager",
)
def nyc_hpd_aep():
    return None

@asset(
    name="nyc_hpd_affordable_building",
    io_manager_key="fastopendata_singlefile__parquet_io_manager",
)
def nyc_hpd_affordable_building():
    return None

@asset(
    name="nyc_hpd_affordable_project",
    io_manager_key="fastopendata_singlefile__parquet_io_manager",
)
def nyc_hpd_affordable_project():
    return None

@asset(
    name="nyc_hpd_conh",
    io_manager_key="fastopendata_singlefile__parquet_io_manager",
)
def nyc_hpd_conh():
    return None

@asset(
    name="nyc_hpd_fee_charges",
    io_manager_key="fastopendata_singlefile__parquet_io_manager",
)
def nyc_hpd_fee_charges():
    return None

@asset(
    name="nyc_hpd_hwo_charges",
    io_manager_key="fastopendata_singlefile__parquet_io_manager",
)
def nyc_hpd_hwo_charges():
    return None

@asset(
    name="nyc_hpd_litigations",
    io_manager_key="fastopendata_singlefile__parquet_io_manager",
)
def nyc_hpd_litigations():
    return None

@asset(
    name="nyc_hpd_omo_charges",
    io_manager_key="fastopendata_singlefile__parquet_io_manager",
)
def nyc_hpd_omo_charges():
    return None

@asset(
    name="nyc_hpd_omo_invoices",
    io_manager_key="fastopendata_singlefile__parquet_io_manager",
)
def nyc_hpd_omo_invoices():
    return None

@asset(
    name="nyc_hpd_registrations",
    io_manager_key="fastopendata_singlefile__parquet_io_manager",
)
def nyc_hpd_registrations():
    return None

@asset(
    name="nyc_hpd_underlying_conditions",
    io_manager_key="fastopendata_singlefile__parquet_io_manager",
)
def nyc_hpd_underlying_conditions():
    return None

@asset(
    name="nyc_hpd_vacate_orders",
    io_manager_key="fastopendata_singlefile__parquet_io_manager",
)
def nyc_hpd_vacate_orders():
    return None

@asset(
    name="nyc_speculation_watch_list",
    io_manager_key="fastopendata_singlefile__parquet_io_manager",
)
def nyc_speculation_watch_list():
    return None
