import datetime
from airflow import models
from airflow.operators import bash_operator

default_dag_args = {
    'start_date': datetime.datetime(2021, 5, 3),
    'email_on_failure': False,
     'email_on_retry': False,
     'retries': 1,
     'retry_delay' : datetime.timedelta(minutes=5)
}

with models.DAG(
        dag_id = 'composer_call_bashoperator_python',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:

    run_extraction = bash_operator.BashOperator(
        task_id='run_extraction',
        bash_command='python3 /home/airflow/gcs/data/Extraction.py')

    run_transformation = bash_operator.BashOperator(
        task_id='run_transformation',
        bash_command='python3 /home/airflow/gcs/data/Transform.py')

    run_extraction >> run_transformation

    