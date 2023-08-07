from datetime import datetime
from airflow.decorators import dag
from include.rt_gtfs.extract_rt import upload_rt_pb_data_to_azure_blob_storage


@dag(dag_id = "realtime_data_retrieval", start_date=datetime(2023,8,1), schedule_interval='*/5 5-21 * * *', catchup=False)
def realtime_data_retrieval():
    upload_realtime_data_to_azure =upload_rt_pb_data_to_azure_blob_storage()

    upload_realtime_data_to_azure

realtime_data_retrieval()