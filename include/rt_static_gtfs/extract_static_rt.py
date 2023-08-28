from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.decorators import task_group
from pathlib import Path
import os
from dotenv import load_dotenv

local_static_data_path = Path(__file__).parents[2].joinpath("./data/static_data_temp")

def _get_static_rt_data(local_static_data_path:Path, api_key_static:str)->None:
    from plugins.gtfs_regio_api import RegioFetcher
    OPERATOR = 'skane'
    api_url_static = "https://opendata.samtrafiken.se/gtfs"
    fetcher = RegioFetcher(OPERATOR, api_key_static,api_url_static)
    fetcher.get_static(local_static_data_path)


def _push_static_zip_to_blob(local_static_data_path:str,account_name:str, container_name:str, shared_access_key:str)->None:
    from azure.storage.blob import BlobServiceClient
    account_url=f"https://{account_name}.blob.core.windows.net"
    blob_service_client = BlobServiceClient(account_url,credential=shared_access_key)
    locally_storred_temp_file = list(local_static_data_path.rglob("*.zip"))[0].as_posix()[-26:]

    if blob_service_client.account_name is not None:
        print("Connection to blob: success")
        try:
            container_client = blob_service_client.get_container_client(container=container_name)
            with open(file=local_static_data_path.joinpath(f'./{locally_storred_temp_file}'), mode="rb") as data:
                blob_client = container_client.upload_blob(name=f"{locally_storred_temp_file}", data=data, overwrite=True)
        except ConnectionError as ce:
            print("the upload to the blob storage failed")


    
@task_group(group_id="upload_static_data_to_azure_blob_storage")
def upload_static_data_to_azure_blob_storage():
    load_dotenv()
    shared_access_key = os.getenv("AZURE_STORAGE_ACCESS_KEY")
    account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    api_key_static = os.getenv("API_KEY_REGIO_STA")

    fetch_static_from_koda_api = PythonOperator(
        task_id = "get_static_rt_data",
        python_callable= _get_static_rt_data,
        op_kwargs = {"local_static_data_path" : local_static_data_path
                     , "api_key_static": api_key_static},
        do_xcom_push = True
    )

    push_static_data_to_blob = PythonOperator(
        task_id = "push_static_data_to_blob",
        python_callable= _push_static_zip_to_blob,
        op_kwargs = {"account_name" : account_name,
                     "shared_access_key": shared_access_key,
                     "local_static_data_path":local_static_data_path,
                     "container_name":"gtfs-static"},
        do_xcom_push = True
    )

    remove_static_local_files = BashOperator(
    task_id =  "remove_static_local_files",
    # Find and delete files staring with skane and ending by .zip
    bash_command = f"cd {local_static_data_path.as_posix()} && rm $(ls | grep -E 'skane.*.zip')"
    )

    trigger_pb_transform_dag = TriggerDagRunOperator(
    task_id ="trigger_data_to_csv_transform",
    trigger_dag_id="static_data_transformation"
    )

    fetch_static_from_koda_api >> push_static_data_to_blob >> remove_static_local_files >> trigger_pb_transform_dag