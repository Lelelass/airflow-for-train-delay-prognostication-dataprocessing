from datetime import datetime
from airflow.decorators import dag

@dag(dag_id = "static_data_transformation", start_date=datetime(2023,7,24), end_date=datetime(2023,7,25), catchup=False)
def static_data_transformation():
    pass

static_data_transformation()