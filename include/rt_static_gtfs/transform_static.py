from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import task_group
from pathlib import Path
import os
from dotenv import load_dotenv

def _insure_data_availability():
    pass


def _retrieve_static_from_blob(account_name:str, container_name:str, shared_access_key:str)->None:
    from azure.storage.blob import BlobServiceClient
    account_url=f"https://{account_name}.blob.core.windows.net"
    blob_service_client = BlobServiceClient(account_url,credential=shared_access_key)
    
    if blob_service_client.account_name is not None:
        print("Connection to blob: success")
        try:
            container_client = blob_service_client.get_container_client(container=container_name)
            with open(file=local_static_data_path.joinpath(f'./{locally_storred_temp_file}'), mode="rb") as data:
                blob_client = container_client.upload_blob(name=f"{locally_storred_temp_file}", data=data, overwrite=True)
        except ConnectionError as ce:
            print("the upload to the blob storage failed")

def _load_gtfs(static_gtfs_zipfile):
    pass

@task_group(group_id="transform_static_data")
def transform_static_data():
    load_dotenv()
    shared_access_key = os.getenv("AZURE_STORAGE_ACCESS_KEY")
    account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")

    _retrieve_static_from_blob = PythonOperator(
        task_id = "retrieve_static_from_blob",
        python_callable=_retrieve_static_from_blob,
        op_kwargs = {"account_name" : account_name,
                     "shared_access_key": shared_access_key,
                     "container_name":"gtfs-static"},
        do_xcom_push = True
    )
