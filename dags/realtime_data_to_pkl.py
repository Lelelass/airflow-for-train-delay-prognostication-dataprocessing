from datetime import datetime
from airflow.decorators import dag


@dag(dag_id = "realtime_data_to_pkl", start_date=datetime(2023,8,9), schedule_interval=None, catchup=False)
def realtime_data_retrieval():
    pass

realtime_data_retrieval()