#!/usr/bin/env python

import os
import datetime

from etl_goes import extract_goes, transform_goes, load_goes
from dagster import asset, define_asset_job, RetryPolicy, Definitions, ScheduleDefinition, IOManager

asset_job = define_asset_job(name="etl_asset_job", selection="*")
asset_schedule = ScheduleDefinition(
    job=asset_job,
    cron_schedule="@hourly",
    execution_timezone="America/New_York"
)

# Optional parameters:
# GOES files are ordered chronologically 
# Default to previous hour if no date env params
dt = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
year = os.getenv("GOES_YEAR", dt.strftime('%Y')
day_of_year = os.getenv("GOES_DOY", dt.strftime('%j'))
hour = os.getenv("GOES_HOUR", dt.strftime('%H'))
# Required parameters:
bucket_name = os.getenv("S3_BUCKET") # Satellite i.e. GOES-18  
product_line = os.getenv("PRODUCT")  # Product line id i.e. ABI...

prefix = product_line + '/' + year + '/' + day_of_year + '/' + hour + '/'

@asset(description="Extract GOES netCDF files from s3 bucket.", compute_kind="python", retry_policy=RetryPolicy(max_retries=3, delay=10))
def source_data(context):
    context.log.info(f"Starting file extract for: {prefix}")
    return extract_goes(bucket_name, prefix, context)
    
@asset(description="Convert GOES netCDF files into csv files.", compute_kind="python", retry_policy=RetryPolicy(max_retries=3, delay=10))
def transformations(context, source_data):
    context.log.info(f"Starting file transform for: {prefix}")
    return transform_goes(source_data, context)

@asset(description="Load GOES csv files into duckdb.", compute_kind="python", retry_policy=RetryPolicy(max_retries=3, delay=10))
def destination(context, transformations):
    context.log.info(f"Starting file load for: {prefix}")
    return load_goes(transformations, context)

# Data assets definitions
defs = Definitions(
    assets=[source_data, transformations, destination],
    jobs=[asset_job],
    schedules=[asset_schedule],
    # Default File IO Manager Resource...
) 
