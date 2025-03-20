"""

This is a script that:

* takes the materialized assets from Dagster (so it assumes the files have been downloaded
locally in the specified folder) and loads them to a bucket in AWS so that Bauplan can create
tables out of them - we assume you have the write permissions to this bucket.
For more info on the import data mechanism for the sandbox, check our docs: https://docs.bauplanlabs.com/en/latest/tutorial/02_catalog.html

* uses the Bauplan client (we assume you have the client installed and a working API key configured)
to create new tables in the sandbox corresponding to the files we just uploaded.

While not intended for production use, this script is a good example of how easy is to use
Bauplan to programmatically handle ingestion and versioning in the cloud of real-world datatets.

This code has been written with Bauplan client: 0.0.3a342

If the API changed, or you need help, please get in touch and we'll be super happy to help: https://www.bauplanlabs.com/

The Bauplan Team, March 2025


---

This folder is self-contained:

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python data_loader.py

---

"""


import boto3
from glob import glob
import os
import bauplan
# global S3 client  
s3_client = boto3.client('s3')
# global Bauplan client
bauplan_client = bauplan.Client()

# global variables
TARGET_S3_BUCKET = 'alpha-hello-bauplan'
TARGET_S3_FOLDER = 'nyc-open-data-week-2025'
TARGET_S3_DESTINATION = f's3://{TARGET_S3_BUCKET}/{TARGET_S3_FOLDER}/'
LOCAL_ASSET_FOLDER = '../data/opendata/'
# match folder with the partition column
TABLE_FOLDERS = [
    ('mta_subway_hourly_ridership', 'transit_timestamp'),
    ('mta_subway_origin_destination_2023', 'origin destination'),
    ('mta_subway_origin_destination_2024', 'origin destination'),
    ('mta_subway_origin_destination_2023', 'created_date')
]
# if we turn this off, we won't upload the files (perhaps they are already there?)
IS_UPLOAD = False

# first we get the files we need to upload from the folder in the project
# NOTE: materialize them with Dagster first
if IS_UPLOAD:
    for table in TABLE_FOLDERS:
        table_path = os.path.join(LOCAL_ASSET_FOLDER, table[0])
        print(f"Uploading table {table[0]} from {table_path}")
        for filename in glob(os.path.join(table_path, '**', '*.parquet'), recursive=True):
            s3_path = f"{TARGET_S3_FOLDER}/{table[0]}/{os.path.basename(filename)}"
            s3_client.upload_file(filename, TARGET_S3_BUCKET, s3_path)
# second, we upload them to the bucket in the relevant subfolder