import csv
import configparser
import os

import requests

# Setup configuration path AND target path.
def extract_data_from_api(tbl):
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


