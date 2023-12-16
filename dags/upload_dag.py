import os
import sys
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta
from airflow.models import DAG
from airflow.utils.dates import days_ago

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
    schedule_interval="0 18 * * *",
    start_date=days_ago(2),
    catchup=False,
    tags=["BigDataProject"],
    default_args=DEFAULT_ARGS
)


task_upload_data = BashOperator(
    task_id='upload_data_to_hadoop',
    bash_command=f'bash {ROOT_PATH}/bash_scipts/load_data.sh'
)

task_upload_data