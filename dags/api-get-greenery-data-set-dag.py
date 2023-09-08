from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils import timezone

from script.main_api_module import extract_data_from_api

table_list = ["addresses", "events", "order-items", "orders", "products", "promos", "users"]

default_args = {
    "owner" : "airflow",
    "start_date" : timezone.datetime(2023, 9 ,8),
}

with DAG(
    dag_id="greenery_kids_test_parsing_argument",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["DEB", "2023", "greenery"]
):
    for table_name in table_list:
        extract_data_parse_args = PythonOperator(
            task_id=f"extract_data_parse_args_{table_name}",
            python_callable=extract_data_from_api,
            op_kwargs={"tbl" : table_name },

        )

    # Task dependencies
    extract_data_parse_args