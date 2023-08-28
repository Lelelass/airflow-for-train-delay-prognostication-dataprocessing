from datetime import datetime
from airflow.decorators import dag
from include.rt_static_gtfs.transform_static import transform_static_data

@dag(dag_id = "static_data_transformation", start_date=datetime(2023,7,26), schedule_interval=None, catchup=False)
def static_data_transformation():
    transform_static_data_from_blob = transform_static_data()

    transform_static_data_from_blob

static_data_transformation()