# Modern Data Orchestration Platform - Dagster SDA

An example using [Dagster Software-Defined Assets](https://docs.dagster.io/concepts/assets/software-defined-assets) to orchestrate a batch ETL pipeline. 
Process downloads [NOAA GOES-R GLM](https://www.goes-r.gov/spacesegment/glm.html) files from AWS s3 bucket, converts them into time series csv 
and loads them to a local backend, persistant duckdb. 

Blog post: [Modern Data Platform Orchestration with Dagster Software-Defined Assets](https://medium.com/@adebayoadejare/modern-data-platform-orchestration-with-dagster-software-defined-assets-6a7182b0d834)

![Alt text](screenshot/dagit_UI_GAL.png "Dagit UI displaying Global Asset Lineage")

## Installation

First make sure, you have the requirements installed, this can be installed from the project directory via pip's requirements.txt:

`pip install -r requirements.txt`

## Quick Start

Run the command to start the dagster orchestration framework: 

`dagster dev -f etl_asset_job.py # Start dagster daemon and dagit ui`

The dagster daemon is required to start the scheduling, from the dagit ui, you can run and monitor the data assets.

## Parametrized Pipeline

Two required parameters, S3_BUCKET and PRODUCT, are configured by default. There is current support for three optional paramters for loading specific available files: GOES_YEAR, GOES_DOY and GOES_HOUR

`export GOES_YEAR=2022; export GOES_DOY=365; export GOES_HOUR=05 # Run the pipeline for Dec 31, 2022 5 AM files`

## Testing 

Use the following command to run tests:

`pytest`

## License

[Apache 2.0 License](LICENSE)