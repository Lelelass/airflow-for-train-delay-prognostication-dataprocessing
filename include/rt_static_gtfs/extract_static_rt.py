from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import task_group
from dotenv import load_dotenv
import os


def _retrieve_blob_service_client(account_name, shared_access_key):
    from azure.storage.blob import BlobServiceClient
    account_url=f"https://{account_name}.blob.core.windows.net"
    blob_service_client = BlobServiceClient(account_url,credential=shared_access_key)

    if blob_service_client.account_name is not None:
        print("Connection success")
        return blob_service_client
    
@task_group(group_id="connect_to_azure_storage")
def connect_to_storage():
    load_dotenv()
    shared_access_key = os.getenv("AZURE_STORAGE_ACCESS_KEY")
    account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")

    establish_connection_to_blob = PythonOperator(
        task_id = "establish_connection_to_blob",
        python_callable= _retrieve_blob_service_client,
        op_kwargs = {"account_name" : account_name
                     , "shared_access_key": shared_access_key},
        do_xcom_push = True
    )

    success_setup = BashOperator(
    task_id =  "setup_success",
    bash_command = f"echo setup data directory successful"
    )

    establish_connection_to_blob >> success_setup