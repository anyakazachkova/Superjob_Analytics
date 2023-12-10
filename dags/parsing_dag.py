import sys
import os
from datetime import timedelta
from typing import NoReturn
from airflow.operators.python import PythonOperator
from airflow.models import DAG
from airflow.utils.dates import days_ago

sys.path.append(
    os.path.join(
        "Superjob_Parser"
    )
)
from datalib.parsers import SuperjobParser
from definitions import ROOT_PATH

DEFAULT_ARGS = {
    "owner": "Anna Kazachkova",
    "email": "anya.kazachkova98@gmail.com",
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1)
}

PARSING_PARAMS = {
    'keywords': 'Python'
}

dag = DAG(
	dag_id='parsing_dag',
    schedule_interval="0 17 * * *",
    start_date=days_ago(2),
    catchup=False,
    tags=["BigDataProject"],
    default_args=DEFAULT_ARGS
)

def parse_data() -> NoReturn:

    parser = SuperjobParser()
    df = parser.parse_vacancies(
        {
            'keywords': 'python'
        }
    )
    parser.save_result(
        df,
        os.path.join(
            ROOT_PATH,
            'results',
            'parsed_data'
        )
    )

task_parse_data = PythonOperator(
    task_id="parse_data", 
    python_callable=parse_data, 
    dag=dag
)