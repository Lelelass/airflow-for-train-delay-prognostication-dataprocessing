# %%

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from pathlib import Path
from airflow.decorators import task_group
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

data_path = Path(__file__).parents[1] / "data"
datalake_path = data_path / "data_lake"
data_warehouse_path = data_path / "data_warehouse"

def _connect_to_storage():
    token_credential = DefaultAzureCredential()

    blob_service_client = BlobServiceClient(
            account_url="https://trainmetricsll.blob.core.windows.net",
            credential=token_credential)
    
    return blob_service_client

blob_service_client = _connect_to_storage()
list(blob_service_client.list_containers())



# %%

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
    establish_connection = PythonOperator(
        task_id = "establish_connection",
        python_callable= _connect_to_storage
    )

    success_setup = BashOperator(
    task_id =  "setup_success",
    bash_command = f"echo setup data directory successful"
    )

    establish_connection >> success_setup
# %%
