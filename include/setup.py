from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import task_group

from pathlib import Path


data_path = Path(__file__).parents[1] / "data"
datalake_path = data_path / "data_lake"
data_warehouse_path = data_path / "data_warehouse"
temps_static_data_path = data_path /"static_data_temp"

    
@task_group(group_id="setup_data_directories")
def setup_directories():
    create_directories = BashOperator(
        task_id = "create_directories",
        bash_command =f"mkdir -p {datalake_path} {data_warehouse_path}",
        )
                                     
    success_setup = BashOperator(
        task_id =  "setup_success",
        bash_command = f"echo setup data directory successful"
    )

    create_directories >> success_setup

