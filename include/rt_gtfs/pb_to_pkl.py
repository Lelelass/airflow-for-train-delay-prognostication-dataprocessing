from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import task_group
from pathlib import Path
import os
from dotenv import load_dotenv
from plugins.merge_stream_pb_with_static import load_protobuf_from_azure
from azure.storage.blob import BlobServiceClient
import datetime as dt
import pytz
import pandas as pd

today = dt.datetime.now(pytz.timezone('Europe/Stockholm')).strftime("%Y-%m-%d")

local_rt_temp_data_path = Path(__file__).parents[2].joinpath("./data/rt_data_temp_transform")

def _list_available_pb_at_day(strorage_account_name:str):
    load_dotenv()
    shared_access_key = os.getenv("AZURE_STORAGE_ACCESS_KEY")

    account_url=f"https://{strorage_account_name}.blob.core.windows.net"
    blob_service_client = BlobServiceClient(account_url,credential=shared_access_key)

    if blob_service_client.account_name is not None:
        print(f"Connection to storage {blob_service_client.account_name}: success")
        container_client = blob_service_client.get_container_client(container="gtfs-realtime")

        pbs_at_day =list(container_client.list_blob_names(name_starts_with = f"skane-TripUpdates-{today}"))
        return pbs_at_day
    
def _get_latest_pb_filename(strorage_account_name:str):
    last_pb_name = _list_available_pb_at_day(strorage_account_name)[-1]
    return last_pb_name

def _load_static_data(strorage_account_name):
    load_dotenv()
    shared_access_key = os.getenv("AZURE_STORAGE_ACCESS_KEY")

    account_url=f"https://{strorage_account_name}.blob.core.windows.net"
    blob_service_client = BlobServiceClient(account_url,credential=shared_access_key)

    if blob_service_client.account_name is not None:
        print(f"Connection to storage {blob_service_client.account_name}: success")
        container_client = blob_service_client.get_container_client(container='gtfs-static-csvs')
    try:
        blob_client = container_client.get_blob_client(filename:=(f"static-{today}.csv"))

        with open(file=local_rt_temp_data_path.joinpath(filename), mode="wb") as sample_blob:

            download_stream = blob_client.download_blob()
            sample_blob.write(download_stream.readall())

    except BrokenPipeError as bp:
        print(bp)


def _load_protobuf_from_azure(task_instance, temp_data_path:Path,strorage_account_name:str):
    last_pb_name = task_instance.xcom_pull(task_ids='get_latest_pb_file'),
    with open(local_rt_temp_data_path.joinpath(f"static-{today}.csv")) as f:
        train_trips_at_day = pd.read_csv(f).drop("Unnamed: 0", axis =1)
        train_trips_at_day = train_trips_at_day.astype({'stop_id' : str, 'trip_id':str})
        
    load_protobuf_from_azure(last_pb_name, temp_data_path, strorage_account_name, train_trips_at_day)


@task_group(group_id="transform_pb_to_pkl")
def transform_pb_to_pkl():
    load_dotenv()
    account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")

    get_latest_pb_file = PythonOperator(
        task_id ="get_latest_pb_filename",
        python_callable= _get_latest_pb_filename,
        op_args= [account_name],
        do_xcom_push = True
    )

    load_static_data = PythonOperator(
         python_callable= _load_static_data,
         op_args= [account_name],
    )

    load_protobuf = PythonOperator(
        task_id = "load_protobuf_from_azure",
        python_callable = _load_protobuf_from_azure,
        op_kwargs = {"temp_data_path" : local_rt_temp_data_path,
                     "strorage_account_name" :account_name}
    )

    remove_temp_local_files = BashOperator(
    bash_command = f"cd {local_rt_temp_data_path.as_posix()} && rm $(ls | grep -E '*.pb|.csv')"     
    )

    get_latest_pb_file >> load_static_data >> load_protobuf >> remove_temp_local_files
