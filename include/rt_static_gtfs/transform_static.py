from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import task_group
from pathlib import Path
import os
from dotenv import load_dotenv
import datetime as dt

temp_data_path = Path("./data/static_data_transform_temp")
today = dt.datetime.now().strftime("%Y-%m-%d")


def _insure_data_availability(list_of_blobs:list)->bool:
    """A check function for insuring that a static file exist from the same day of execution. Returns a bool at first tuple entry for the outcome of _retrieve_static_from_blob execution. Returns a Filename as the second tuple entry if the validity of data in insured """
    import datetime as dt
    import re
    today = dt.datetime.now().strftime("%Y-%m-%d")
    if len((matches:=[blob_name for blob_name in list_of_blobs if re.search(today, blob_name) is not None])) > 0:
        return True, matches[0]
    else:
        return False, None


def _retrieve_static_from_blob(account_name:str, container_name:str, shared_access_key:str)->None:
    """Retrieve the daily static zip file from an Azure blob and store it in a temp folder on the host"""
    from azure.storage.blob import BlobServiceClient
    account_url=f"https://{account_name}.blob.core.windows.net"
    blob_service_client = BlobServiceClient(account_url,credential=shared_access_key)
    
    if blob_service_client.account_name is not None:

        print("Connection to blob: success")

        try:

            container_client = blob_service_client.get_container_client(container=container_name)
            data_availability, filename = _insure_data_availability(list(container_client.list_blob_names()))

            if data_availability:
                try:
                    blob_client = container_client.get_blob_client(filename)

                    with open(file=temp_data_path.joinpath(filename), mode="wb") as sample_blob:

                        download_stream = blob_client.download_blob()
                        sample_blob.write(download_stream.readall())

                except BrokenPipeError as bp:
                    print(bp)

        except ConnectionError as ce:
            print(f"Reading the daily zip in blob storage failed. Data avilability = {data_availability} - {ce}")


def _load_gtfs()->tuple:
    """ Uses the gtfs functions module to output a raw gtfs static zip into exploitable Pandas DataFrames, routes and stops are returned in a tuple"""
    from gtfs_functions import Feed
    gtfs_path = list(temp_data_path.rglob("*.zip"))[0]
    feed = Feed(gtfs_path)
    routes = feed.routes
    stops = feed.stop_times
    return routes, stops

def _get_train_trips_at_day(routes, stops)->object:
    """Filters train routes from all routes in the GTFS and return a dataframe of the daily train trips"""
    TRAIN_OPERATORS = ('Pågatåg','PågatågExpress','Krösatåg','Öresundståg')
    trafficked_train_routes_at_day = routes.where(routes.route_desc.isin(TRAIN_OPERATORS)).dropna(how='all')
    train_trips_at_day = stops.where(stops.route_id.isin(trafficked_train_routes_at_day.route_id)).dropna(how = "all")
    train_trips_at_day.drop(["pattern","timepoint", "geometry", "shape_id", "parent_station", "location_type", "service_id"], axis=1, inplace=True)
    return train_trips_at_day

def _train_trips_at_day_to_csv(train_trips_at_day)->None:
    import datetime as dt
    import pandas as pd
    csv_filepath = temp_data_path.joinpath(f"./static-{today}.csv")
    train_trips_at_day.to_csv(csv_filepath)
    return csv_filepath
    

def _store_train_trips_at_day_csv_blob(train_trips_csv_fpath:Path, account_name:str, container_name:str, shared_access_key:str)->None:
    """Push the daily train trips to the storage account container as a csv blob"""
    from azure.storage.blob import BlobServiceClient
    account_url=f"https://{account_name}.blob.core.windows.net"

    blob_service_client = BlobServiceClient(account_url,credential=shared_access_key)
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(train_trips_csv_fpath.parts[-1])
    blob_client.upload_blob(train_trips_csv_fpath.as_posix(), overwrite=True)



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
        do_xcom_push = False
    )

    load_gtfs = PythonOperator(
        task_id ="load_static_regional_gtfs",
        python_callable=_load_gtfs,
        do_xcom_push = True
    )

    get_train_trips_at_day = PythonOperator(
        task_id ="get_train_trips_at_day",
        python_callable= _get_train_trips_at_day,
        op_args=[],
        do_xcom_push=True
    )

    train_trips_at_day_to_csv = PythonOperator(
        task_id = "save_train_trips_at_day_as_csv",
        python_callable=_train_trips_at_day_to_csv,
        op_args=[]
    )

    store_train_trips_at_day_csv_blob = PythonOperator(
        task_id= "store_train_trips_at_day_to_csv_blob",
        python_callable=_store_train_trips_at_day_csv_blob,
        op_kwargs = {"account_name" : account_name,
                "shared_access_key": shared_access_key,
                "container_name":"gtfs-static-csvs"}
    )

    clear_temp_data = BashOperator()


    retrieve_static_from_blob >> load_gtfs >> get_train_trips_at_day >> train_trips_at_day_to_csv>> store_train_trips_at_day_csv_blob >> clear_temp_data
