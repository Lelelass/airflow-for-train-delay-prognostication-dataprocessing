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
    import re
    last_pb_name = task_instance.xcom_pull(task_ids="transform_pb_to_pkl.get_latest_pb_filename")
    with open(local_rt_temp_data_path.joinpath(f"static-{today}.csv")) as f:
        train_trips_at_day = pd.read_csv(f).drop("Unnamed: 0", axis =1)
        train_trips_at_day = train_trips_at_day.astype({'stop_id' : str, 'trip_id':str})
        
    df = load_protobuf_from_azure(last_pb_name, temp_data_path, strorage_account_name, train_trips_at_day)
    df.to_pickle(local_rt_temp_data_path.joinpath(re.sub('pb', 'pkl', last_pb_name)))

def _push_pkl_to_blob(local_rt_data_path,account_name:str, container_name:str)->None:
    from azure.storage.blob import BlobServiceClient
    load_dotenv()
    shared_access_key = os.getenv("AZURE_STORAGE_ACCESS_KEY")
    account_url=f"https://{account_name}.blob.core.windows.net"
    blob_service_client = BlobServiceClient(account_url,credential=shared_access_key)
    locally_storred_temp_file = list(local_rt_data_path.rglob("*.pkl"))[0].as_posix()[-38:]
    if blob_service_client.account_name is not None:
        print("Connection to blob: success")
        try:
            container_client = blob_service_client.get_container_client(container=container_name)
            with open(file=local_rt_data_path.joinpath(f'./{locally_storred_temp_file}'), mode="rb") as data:
                container_client.upload_blob(name=f"{locally_storred_temp_file}", data=data, overwrite=True)
        except ConnectionError as ce:
            print(f"the upload to the blob storage failed : {ce}")


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
        task_id = "load_static_data",
        python_callable= _load_static_data,
        op_args= [account_name],
    )

    load_protobuf = PythonOperator(
        task_id = "load_protobuf_from_azure",
        python_callable = _load_protobuf_from_azure,
        op_kwargs = {"temp_data_path" : local_rt_temp_data_path,
                     "strorage_account_name" :account_name}
    )

    push_pkl_to_blob = PythonOperator(
        task_id = "push_pkl_to_blob",
        python_callable= _push_pkl_to_blob,
        op_kwargs={'local_rt_data_path':local_rt_temp_data_path,
                   'account_name':account_name, 'container_name':'gtfs-realtime-pkl'
                   }
    )
    remove_temp_local_files = BashOperator(
        task_id = "remove_temp_local_files",    
        bash_command = f"cd {local_rt_temp_data_path.as_posix()} && rm $(ls | grep -E '*.pb|.csv|.pkl')"     
    )

    get_latest_pb_file >> load_static_data >> load_protobuf >> push_pkl_to_blob >> remove_temp_local_files
