from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from process_data import download_and_transform

default_args = {
    "owner": "airflow",
    "email": ["tpgoncalvesc@gmail.com"],
    "email_on_fail": False,
    "email_on_retry": False,
    "retry_interval": timedelta(seconds=30),
}

with DAG(
    dag_id="small_project_dag",
    default_args=default_args,
    star_date=datetime(2023, 1, 1)
    schedule_interval="@daily"
    catchup=False
) as dag:

    output_file_path = os.path.join("airflow", "data", "drinks_processed.csv")


    download_transform_task = PythonOperator(
        task_id="download_transform",
        python_callable=download_and_transform,
        op_kwargs={"output_path": output_file_path},
    )

    check_file_task = BashOperator(
        task_id="check_file",
        bash_command=f"ls -lh {output_file_path} && echo 'File Founded!'"
    )

    # ------------------------------------------------------
    # Definindo a ordem de execução (pipeline)
    # ------------------------------------------------------

    download_transform_task >> check_file_task