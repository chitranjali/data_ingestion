from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

local_workflow = DAG(
    "LocalIngestionDAG")

with local_workflow:
    curl_task = BashOperator(
        bash_command = 'echo "hello world"'
    )

    ingest_task = BashOperator(
        task_id = 'ingest',
        bash_command = 'echo "ingeting"'
    )

    # Workflow
    curl_task >> ingest_task