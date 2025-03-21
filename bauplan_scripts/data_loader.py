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
from tqdm import tqdm
# global S3 client  
s3_client = boto3.client('s3')
# global Bauplan client
bauplan_client = bauplan.Client(profile='prod')

# global variables
TARGET_S3_BUCKET = 'alpha-hello-bauplan'
TARGET_S3_FOLDER = 'nyc-open-data-week-2025'
TARGET_S3_DESTINATION = f's3://{TARGET_S3_BUCKET}/{TARGET_S3_FOLDER}/'
LOCAL_ASSET_FOLDER = '../data/opendata/'
BAUPLAN_TABLE_NAMESPACE = 'nyc_open_data'
# match folder with the partition column
TABLE_FOLDERS = [
    ('mta_subway_hourly_ridership', 'transit_timestamp'),
    ('mta_subway_origin_destination_2023', 'timestamp'),
    ('mta_subway_origin_destination_2024', 'timestamp'),
    ('nyc_threeoneone_requests', 'created_date')
]
# if we turn this off, we won't upload the files (perhaps they are already there?)
IS_UPLOAD = False

# first we get the files we need to upload from the folder in the project
# and upload them to the bucket in the relevant subfolder (one folder one table)
# NOTE: materialize them with Dagster first
if IS_UPLOAD:
    for table in TABLE_FOLDERS:
        table_path = os.path.join(LOCAL_ASSET_FOLDER, table[0])
        print(f"Uploading table {table[0]} from {table_path}")
        for filename in tqdm(glob(os.path.join(table_path, '**', '*.parquet'), recursive=True)):
            s3_path = f"{TARGET_S3_FOLDER}/{table[0]}/{os.path.basename(filename)}"
            s3_client.upload_file(filename, TARGET_S3_BUCKET, s3_path)
            
# second, we use the WAP pattern with Bauplan to load each table in a safe way
# i.e. using data branches - to know more about the pattern, see for example:
# https://www.prefect.io/blog/prefect-on-the-lakehouse-write-audit-publish-pattern-with-bauplan

# read the user info from the bauplan profile programmatically
user = bauplan_client.info().user
username = user.username
# make sure we have all the info available
assert username is not None and user is not None
# important global branch / table variables
source_branch_name = 'main'
my_branch_name_prefix = f'{username}.open_data_2025_'
# let's import one table at a time
for table in TABLE_FOLDERS:
    print(f"\n\n====> Processing table {table[0]}")
    table_name = table[0]
    partition_col = table[1]
    # create a branch for the table
    branch_name = f'{my_branch_name_prefix}{table[0]}'
    # clean up the branch if it exists
    if bauplan_client.has_branch(branch_name):
        bauplan_client.delete_branch(branch_name)
     
    bauplan_client.create_branch(branch_name, source_branch_name)
    # if not exists, create the namespace
    if not bauplan_client.has_namespace(BAUPLAN_TABLE_NAMESPACE, branch_name):
        bauplan_client.create_namespace(BAUPLAN_TABLE_NAMESPACE, branch_name)
    # create the table...
    search_uri = f"{TARGET_S3_DESTINATION}{table_name}/*.parquet"
    print(f"Creating table {table_name} with uri {search_uri}")
    bauplan_client.create_table(
        table=table_name,
        search_uri=search_uri,
        branch=branch_name,
        namespace=BAUPLAN_TABLE_NAMESPACE,
        partitioned_by=f'DAY({partition_col})',
        # since this is a demo, we replace the table if it exists
        replace=True
    )
    # ...and import the data
    print(f"Importing data for table {table_name}")
    plan_state = bauplan_client.import_data(
        table=table_name,
        search_uri=search_uri,
        branch=branch_name,
        namespace=BAUPLAN_TABLE_NAMESPACE,
        client_timeout=60*10
    )
    if plan_state.error:
        raise Exception(f"Error importing data for table {table_name}: {plan_state.error}")
    # if all went well, we can merge the branch and delete it
    print(f"Merging branch {branch_name}")
    bauplan_client.merge_branch(branch_name, source_branch_name)
    print(f"Deleting branch {branch_name}")
    bauplan_client.delete_branch(branch_name)
    print(f"\n\n====> Processing done for {table[0]}")
    
    
print("All done!")