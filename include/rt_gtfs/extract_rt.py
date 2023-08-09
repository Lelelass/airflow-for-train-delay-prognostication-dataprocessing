from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.decorators import task_group
from pathlib import Path
import os
from dotenv import load_dotenv

local_rt_temp_data_path = Path(__file__).parents[2].joinpath("./data/rt_data_temp")

def _get_rt_data(local_rt_data_path:Path, api_key_rt:str)->None:
    from plugins.gtfs_regio_api import RegioFetcherRt
    OPERATOR = 'skane'
    FEED = 'TripUpdates'
    api_url_rt = "https://opendata.samtrafiken.se/gtfs-rt"
    fetcher = RegioFetcherRt(OPERATOR, FEED, api_key_rt, api_url_rt)
    fetcher.get_realtime(local_rt_data_path)

def _push_rt_to_blob(local_rt_data_path,account_name:str, container_name:str, shared_access_key:str)->None:
    from azure.storage.blob import BlobServiceClient
    account_url=f"https://{account_name}.blob.core.windows.net"
    blob_service_client = BlobServiceClient(account_url,credential=shared_access_key)
    locally_storred_temp_file = list(local_rt_data_path.rglob("*.pb"))[0].as_posix()[-37:]
    if blob_service_client.account_name is not None:
        print("Connection to blob: success")
        try:
            container_client = blob_service_client.get_container_client(container=container_name)
            with open(file=local_rt_data_path.joinpath(f'./{locally_storred_temp_file}'), mode="rb") as data:
                container_client.upload_blob(name=f"{locally_storred_temp_file}", data=data, overwrite=True)
        except ConnectionError as ce:
            print(f"the upload to the blob storage failed : {ce}")

@task_group(group_id="upload_rt_pb_data_to_azure_blob_storage")
def upload_rt_pb_data_to_azure_blob_storage():
    load_dotenv()
    shared_access_key = os.getenv("AZURE_STORAGE_ACCESS_KEY")
    account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    api_key_rt = os.getenv("API_KEY_REGIO_RT")

    fetch_rt_from_koda_api = PythonOperator(
        task_id = "get_realtime_pb_data",
        python_callable= _get_rt_data,
        op_kwargs = {"local_rt_data_path" : local_rt_temp_data_path
                     , "api_key_rt": api_key_rt}
    )

    push_rt_data_to_blob = PythonOperator(
    task_id = "push_realtime_pb_data_to_blob",
    python_callable= _push_rt_to_blob,
    op_kwargs = {"account_name" : account_name,
                    "shared_access_key": shared_access_key,
                    "local_rt_data_path":local_rt_temp_data_path,
                    "container_name":"gtfs-realtime"}
    )

    remove_rt_pb_local_files = BashOperator(
    task_id =  "remove_rt_pb_local_files",
    # Find and delete files staring with skane and ending by .zip
    bash_command = f"cd {local_rt_temp_data_path.as_posix()} && rm $(ls | grep -E '*.pb')"
    )

    trigger_pb_transform_dag = TriggerDagRunOperator(
        trigger_dag_id="realtime_data_to_pkl"
    )

    fetch_rt_from_koda_api >> push_rt_data_to_blob >> remove_rt_pb_local_files >> trigger_pb_transform_dag