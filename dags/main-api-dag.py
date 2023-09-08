from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils import timezone

from google.cloud import storage
from google.oauth2 import service_account

import csv
import configparser
import os
import json

import requests

# Setup configuration path AND target path.
tbl = "addresses"

def extract_data_from_api():
    root_path = os.getcwd()
    config_path = f"{root_path}/env_conf"
    pipeline_config_path = f"{config_path}/pipeline.conf"

    DATA_FOLDER = "data"
    if not os.path.exists(f"{root_path}/{DATA_FOLDER}"):
        os.mkdir(f"{root_path}/{DATA_FOLDER}")

    # create configObject to read configuration from pipeline.conf file
    config = configparser.ConfigParser()
    config.read(pipeline_config_path)
    host = config.get("api_config", "host")
    port = config.get("api_config", "port")

    API_URL = f"http://{host}:{port}"

    # table_list = ["addresses", "events", "order-items", "orders", "products", "promos", "users"]

    # for tbl in table_list:
        # print(f"{API_URL}/{tbl}")
    response = requests.get(f"{API_URL}/{tbl}")
    data = response.json()

    with open(f"{DATA_FOLDER}/{tbl}.csv", "w", newline='') as f:
        writer = csv.writer(f)
        header = data[0].keys()
        # print(writer)
        # print(header)
        writer.writerow(header)

        for each in data:
            writer.writerow(each.values())


# ref: https://gcloud.readthedocs.io/en/latest/storage-buckets.html
def import_csv_to_gcs():

    # Setup configuration path AND target path.
    table_name = "addresses"
    # Setup configuration path AND target path.
    root_path = os.getcwd()
    config_path = f"{root_path}/env_conf"
    keyfile = f"{config_path}/greenery-google-cloud-storage.json"
    DATA_FOLDER = f"{root_path}/data"
    BUSINESS_DOMAIN = "greenery-test-airflow"

    # Setup credentails for GoogleCloudStorage Connection
    # ref: https://google-auth.readthedocs.io/en/master/reference/google.oauth2.service_account.html
    service_account_info = json.load(open(keyfile))
    credentials = service_account.Credentials.from_service_account_info(service_account_info)

    # table_name= file_name.split(".csv")[0]
    project_id = "greenery-398007"
    bucket_name = "greenery-bucket"
    source_file_path = f"{DATA_FOLDER}/{table_name}.csv"
    destination_blob_name = f"{BUSINESS_DOMAIN}/{table_name}/{table_name}.csv"

    # Connect to GoogleCloudStorage
    # ref: https://gcloud.readthedocs.io/en/latest/storage-client.html
    storage_client = storage.Client(project=project_id, credentials=credentials)

    # ref: https://gcloud.readthedocs.io/en/latest/storage-blobs.html
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_path)

    print(f"File {table_name} uploaded to GoogleCloudStorage: {destination_blob_name}.")


default_args = {
    "owner": "airflow",
    "start_date": timezone.datetime(2023, 9, 7),
}


with DAG(
    dag_id="greenery_kids_data_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["DEB", "2023", "greenery"],
):

    # Extract data from Postgres, API, or SFTP
    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data_from_api,
    )

    # Extract data from Postgres, API, or SFTP
    import_data_to_gcs = PythonOperator(
        task_id="import_data_to_gcs",
        python_callable=import_csv_to_gcs,
    )

    # Task dependencies
    extract_data >> import_data_to_gcs