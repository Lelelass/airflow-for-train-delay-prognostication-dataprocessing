from datetime import datetime
from airflow.decorators import dag
from include.rt_gtfs.pb_to_pkl import transform_pb_to_pkl

@dag(dag_id = "realtime_data_to_pkl", start_date=datetime(2023,8,9), schedule_interval=None, catchup=False)
def realtime_data_retrieval():
    transform_pb_to_pkl_and_store = transform_pb_to_pkl()

    transform_pb_to_pkl_and_store
    
realtime_data_retrieval()