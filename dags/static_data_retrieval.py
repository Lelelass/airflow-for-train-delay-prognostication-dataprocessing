from datetime import datetime
from airflow.decorators import dag
from include.rt_static_gtfs.extract_static_rt import upload_static_data_to_azure_blob_storage


@dag(dag_id = "static_data_retrieval", start_date=datetime(2023,7,27), schedule_interval='0 5 * * *', catchup=False)
def static_data_retrieval():
    upload_static_data_to_azure =upload_static_data_to_azure_blob_storage()

    upload_static_data_to_azure

static_data_retrieval()