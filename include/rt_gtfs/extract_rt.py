from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import task_group
from pathlib import Path
import os
from dotenv import load_dotenv
import datetime as dt
import pandas as pd

temp_data_path = Path(__file__).parents[2].joinpath("./data/rt_data_temp")

def _get_rt_data(local_rt_data_path:Path, api_key_rt:str)->None:
    from plugins.gtfs_regio_api import RegioFetcherRt
    OPERATOR = 'skane'
    FEED = 'TripUpdates'
    api_url_rt = "https://opendata.samtrafiken.se/gtfs-rt"
    fetcher = RegioFetcherRt(OPERATOR, FEED, api_key_rt, api_url_rt)
    fetcher.get_realtime(local_rt_data_path)

def _push_rt_pb_to_blob(local_rt_data_path:str,account_name:str, container_name:str, shared_access_key:str)->None:
    from azure.storage.blob import BlobServiceClient
    account_url=f"https://{account_name}.blob.core.windows.net"
    blob_service_client = BlobServiceClient(account_url,credential=shared_access_key)
    #TODO handle pb file
    #locally_storred_temp_file = list(local_rt_data_path.rglob("*.zip"))[0].as_posix()[-26:]

    if blob_service_client.account_name is not None:
        print("Connection to blob: success")
        try:
            container_client = blob_service_client.get_container_client(container=container_name)
            with open(file=local_rt_data_path.joinpath(f'./{locally_storred_temp_file}'), mode="rb") as data:
                blob_client = container_client.upload_blob(name=f"{locally_storred_temp_file}", data=data, overwrite=True)
        except ConnectionError as ce:
            print("the upload to the blob storage failed")