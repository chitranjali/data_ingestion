from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import os
from datetime import datetime
from ingest_script import ingest_callable

# variable will default to second parameter if can't find environmental variable
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', "/opt/airflow")

PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')


local_workflow = DAG(
    "LocalIngestionDAG",
    schedule_interval = "0 6 2 * *",
    start_date = datetime(2021, 1, 1)
)

# url = "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv"
# url="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

URL_PREFIX = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow"
URL_TEMPLATE = URL_PREFIX + "/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz"
# URL_TEMPLATE = URL_PREFIX + "/yellow_tripdata_2021-01.csv.gz"
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + "/output_{{ execution_date.strftime(\'%Y-%m\') }}.csv"
# OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + "/output_2021-01.csv"
TABLE_NAME_TEMPLATE = 'yellow_taxi_{{ execution_date.strftime(\'%Y_%m\') }}'
# TABLE_NAME_TEMPLATE = 'yellow_taxi_2021_01'

with local_workflow:
    curl_task = BashOperator(
        task_id = 'curl',
        bash_command = f'curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}'
    )

    # We'll pass argument using the env file.
    ingest_task = PythonOperator(
        task_id = 'ingest',
        python_callable = ingest_callable,
        op_kwargs = dict(user=PG_USER, password=PG_PASSWORD, host=PG_HOST, port=PG_PORT, db=PG_DATABASE, table_name=TABLE_NAME_TEMPLATE, csv_file=URL_TEMPLATE),
    )

    # Workflow
    curl_task >> ingest_task