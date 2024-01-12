import os
import sys
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.utils.dates import days_ago
import pendulum

sys.path.append(
    os.path.join(
        "Superjob_Parser"
    )
)
from definitions import ROOT_PATH


DEFAULT_ARGS = {
    "owner": "Anna Kazachkova",
    "email": "anya.kazachkova98@gmail.com",
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
	dag_id='upload_data_dag',
    schedule_interval="00 23 * * *",
    start_date=pendulum.datetime(2023, 12, 22, tz="Europe/Moscow"),
    catchup=False,
    tags=["BigDataProject"],
    default_args=DEFAULT_ARGS
)


task_upload_data = BashOperator(
    task_id='upload_data_to_hadoop',
    bash_command=f'bash {ROOT_PATH}/bash_scripts/load_data.sh ',
    dag=dag
)

task_calculate_metrics = BashOperator(
    task_id='calculate_metrics',
    bash_command=f'bash {ROOT_PATH}/bash_scripts/calculate_metrics.sh ',
    dag=dag
)

task_get_metrics = BashOperator(
    task_id='get_metrics',
    bash_command=f'bash {ROOT_PATH}/bash_scripts/get_metrics.sh ',
    dag=dag
)

task_upload_data >> task_calculate_metrics >> task_get_metrics