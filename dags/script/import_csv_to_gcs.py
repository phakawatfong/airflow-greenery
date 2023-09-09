import json
import os

from google.cloud import storage
from google.oauth2 import service_account

# Setup configuration path AND target path.
root_path = os.getcwd()
config_path = f"{root_path}/env_conf"
keyfile = f"{config_path}/greenery-google-cloud-storage.json"
DATA_FOLDER = f"{root_path}/data"
BUSINESS_DOMAIN = "greenery-airflow-automated"

# Setup credentails for GoogleCloudStorage Connection
# ref: https://google-auth.readthedocs.io/en/master/reference/google.oauth2.service_account.html
service_account_info = json.load(open(keyfile))
credentials = service_account.Credentials.from_service_account_info(service_account_info)

# ref: https://gcloud.readthedocs.io/en/latest/storage-buckets.html
def import_csv_to_gcs(file_name):
    table_name = file_name
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


print(os.listdir(DATA_FOLDER))
greenery_file_list = list(os.listdir(DATA_FOLDER))

# for table_name in greenery_file_list:
#     import_csv_to_gcs(table_name)

