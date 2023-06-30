from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from pathlib import Path
from airflow.decorators import task_group
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os

data_path = Path(__file__).parents[1] / "data"
datalake_path = data_path / "data_lake"
data_warehouse_path = data_path / "data_warehouse"

def _retrieve_blob_service_client(account_name):
    load_dotenv()
    account_url=f"https://{account_name}.blob.core.windows.net"
    shared_access_key = os.getenv("AZURE_STORAGE_ACCESS_KEY")

    blob_service_client = BlobServiceClient(account_url,credential=shared_access_key)
    
    return blob_service_client

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


@task_group(group_id="connect_to_azure_storage")
def connect_to_storage():

    establish_connection_to_blob = PythonOperator(
        task_id = "establish_connection_to_blob",
        python_callable= _retrieve_blob_service_client,
        op_kwargs = {"account_name" : "trainmetricsll"}
    )

    success_setup = BashOperator(
    task_id =  "setup_success",
    bash_command = f"echo setup data directory successful"
    )

    establish_connection_to_blob >> success_setup
