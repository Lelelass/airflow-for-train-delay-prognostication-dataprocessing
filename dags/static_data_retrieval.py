from datetime import datetime
from airflow.decorators import dag, task_group, task
from include.setup import setup_directories
from include.rt_static_gtfs.extract import extract_queue_time
from include.rt_static_gtfs.load import load_datalake

@dag(dag_id = "queue_time_ELT", start_date=datetime(2023,6,8), end_date=datetime(2023,6,11), catchup=False)
def queue_time_ELT():
    setup = setup_directories()
    extract_queue_time_ = extract_queue_time()
    load_queue_time = load_datalake()


    # dummy parrallel task group
    @task_group
    def airquality_extract():
        @task(task_id="extract")
        def extract():
            return "temperature dummy"
        
    setup >> extract_queue_time_ >> load_queue_time

    setup >> airquality_extract()

queue_time_ELT()