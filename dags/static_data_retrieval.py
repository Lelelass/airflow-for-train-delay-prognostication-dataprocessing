from datetime import datetime
from airflow.decorators import dag, task_group, task
from include.setup import connect_to_storage

@dag(dag_id = "static_data_retrieval", start_date=datetime(2023,6,28), end_date=datetime(2023,7,1), catchup=False)
def static_data_retrieval():
    setup = connect_to_storage()

    setup

static_data_retrieval()