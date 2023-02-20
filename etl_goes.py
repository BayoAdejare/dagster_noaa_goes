#!/usr/bin/env python

import os
import shutil
import duckdb

import netCDF4 as nc
import pandas as pd

from botocore import UNSIGNED, exceptions
from botocore.client import Config
from boto3 import client
from pathlib import Path


# Extract GOES files from s3
def extract_goes(bucket: str, prefix: str, context: str=None) -> pd.DataFrame:
    """
    Downloads GOES netCDF files from s3 buckets
    prefix = s3://<weather_satellite>/<product_line>/<year>/<day_of_year>/<hour>/<OR_...*.nc>
    """
    dest_folder = os.path.join(os.getcwd(), 'goesExtract')
    # Create folder if not existing
    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)
    # Navigate to folder
    os.chdir(dest_folder)
    # Configure s3 no sign in credential
    s3 = client('s3', config=Config(signature_version=UNSIGNED))
    # List existing files in buckets
    for files in s3.list_objects(Bucket=bucket, Prefix=prefix)['Contents']:
        # Download file from list
        path, filename = os.path.split(files['Key'])
        try:
            s3.download_file(Bucket=bucket,Filename=filename,Key=files['Key'])
        except exceptions.ClientError as err:
            if err.response['Error']['Code'] == "404":
                print(f"{filename} cannot be located.")
            else:
                raise
    # List files downloaded
    df_extract = pd.DataFrame(os.listdir())
    return df_extract
    

# Transform GOES into python objects
def transform_goes(extract_goes: str=None, context: str=None) -> pd.DataFrame: 
    """
    Convert GOES netCDF files into csv
    """
    source_folder = os.path.join(os.getcwd(), 'goesExtract')
    dest_folder = os.path.join(os.getcwd(), 'goesTransform')
    glm_files = os.listdir(source_folder)
    # Exit if source folder not existing
    if not os.path.exists(source_folder):
        pass
    # Empty destination folder or recreate
    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)
    else:
        filelist = [ f for f in os.listdir(dest_folder) if f.endswith(".csv") ]
        for f in filelist:
            os.remove(os.path.join(dest_folder, f))
    # Convert glm files into one time series dataframe
    for filename in glm_files:
        src_filename = Path(os.path.join(source_folder, filename))
        dest_filename = src_filename.with_suffix('').with_suffix('.csv')
        # Create dataset
        glm =  nc.Dataset(src_filename, mode='r')
        # flash_lat = glm.variables['flash_lat'][:]
        # flash_lon = glm.variables['flash_lon'][:]
        flash_time = glm.variables['flash_time_offset_of_first_event']
        flash_energy = glm.variables['flash_energy'][:]
        dtime = nc.num2date(flash_time[:],flash_time.units)
        # Flatten multi-dimensional data into series        
        flash_energy_ts = pd.Series(flash_energy, index=dtime)
        # Headers 
        with open(dest_filename, 'w') as glm_file:
            glm_file.write('flash_date_time,flash_energy\n')
        # Write to csv
        flash_energy_ts.to_csv(dest_filename, index=True, header=False, mode='a')
        # Move files
        shutil.move(dest_filename, dest_folder) 
    # List converted files
    df_transform = pd.DataFrame(os.listdir())
    return df_transform

# Load files into destination
def load_goes(transform_goes: str=None, context: str=None) -> pd.DataFrame:
    """
    Load GOES csv files into destination system
    """ 
    source_folder = os.path.join(os.getcwd(), 'goesTransform')
    dest_folder = os.path.join(os.getcwd(), 'goesLoad')
    glm_files = os.listdir(source_folder)
    # Create folder if not existing
    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)
    # Navigate to source
    os.chdir(source_folder)
    # Create in-memory db
    conn = duckdb.connect(f"{dest_folder}/flashdb.db")
    try:
        for filename in glm_files:
            # Copy to folder
            shutil.copy(filename, dest_folder)
    except Exception as e:
        print("Exception errors received.")
    try:
        # create the table "flash_tbl" from the csv files
        conn.execute(f"CREATE TABLE flash_tbl AS SELECT * FROM read_csv_auto('*.csv', header=True, filename=True);")
    except Exception as db_e:
        # table likely exist try insert
        conn.execute(f"INSERT INTO flash_tbl SELECT * FROM read_csv_auto('*.csv', header=True, filename=True);")
    try:
        # Navigate to destination
        os.chdir(dest_folder)
        os.makedirs('loaded')
        for filename in glm_files:
            # Move loaded files
            print(f"Filename: {filename}")
            shutil.move(os.path.join(os.getcwd(), filename), "loaded")
    except Exception as e:
        print("Exception errors received.")
    # cleanup
    shutil.rmtree('loaded')
    # List files loaded
    df_load = conn.execute("SHOW TABLES;").df()
    return df_load