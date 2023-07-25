from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import task_group
from pathlib import Path
import os
from dotenv import load_dotenv

temp_data_path = Path("./data/static_data_transform_temp")


def _insure_data_availability(list_of_blobs:list)->bool:
    import datetime as dt
    import re
    today = dt.datetime.now().strftime("%Y-%m-%d")
    if len((matches:=[blob_name for blob_name in list_of_blobs if re.search(today, blob_name) is not None])) > 0:
        return True, matches[0]
    else:
        return False, None


def _retrieve_static_from_blob(account_name:str, container_name:str, shared_access_key:str)->None:
    from azure.storage.blob import BlobServiceClient
    account_url=f"https://{account_name}.blob.core.windows.net"
    blob_service_client = BlobServiceClient(account_url,credential=shared_access_key)
    
    if blob_service_client.account_name is not None:

        print("Connection to blob: success")

        try:

            container_client = blob_service_client.get_container_client(container=container_name)
            data_availability, filename = _insure_data_availability(list(container_client.list_blob_names()))

            if data_availability:

                blob_client = container_client.get_blob_client(filename)

                with open(file=temp_data_path.joinpath(filename), mode="wb") as sample_blob:

                    download_stream = blob_client.download_blob()
                    sample_blob.write(download_stream.readall())

        except ConnectionError as ce:
            print("Reading the daily zip in blob storage failed")


def _load_gtfs(static_gtfs_zipfile):
    pass

@task_group(group_id="transform_static_data")
def transform_static_data():
    load_dotenv()
    shared_access_key = os.getenv("AZURE_STORAGE_ACCESS_KEY")
    account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")


    retrieve_static_from_blob = PythonOperator(
        task_id = "retrieve_static_from_blob",
        python_callable=_retrieve_static_from_blob,
        op_kwargs = {"account_name" : account_name,
                     "shared_access_key": shared_access_key,
                     "container_name":"gtfs-static"},
        do_xcom_push = True
    )

    retrieve_static_from_blob
