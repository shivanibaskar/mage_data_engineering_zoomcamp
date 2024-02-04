from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
from os import path
import pyarrow as pa
import pyarrow.parquet as pq
import os
import pyarrow.dataset as ds



if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] ="/home/src/absolute-axis-409317-a3c8eccfa934.json"
bucket_name = 'absolute-axis-409317-terra-bucket'
object_key = 'ny_green_taxi_data.parquet'
project_id = "absolute-axis-409317"
table_name = "nyc_green_taxi_data"
root_path = f'{bucket_name}/{table_name}'
# Write your data as Parquet files to a bucket in GCP, partioned by lpep_pickup_date. Use the pyarrow library!
@data_exporter
def export_data(data , *args,**kwargs):
    

    table = pa.Table.from_pandas(data)

    gcs = pa.fs.GcsFileSystem()

    # Create a dataset

    pq.write_to_dataset(
        table,
        root_path,
        partition_cols = ['lpep_pickup_date'],
        filesystem = gcs)

   
