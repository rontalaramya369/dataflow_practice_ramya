from airflow import DAG

from airflow.operators.bash import BashOperator

from datetime import datetime


with DAG(

    dag_id='crypto_pipeline',

    start_date=datetime(2026,1,1),

    schedule=None,

    catchup=False

) as dag:


    hello = BashOperator(

        task_id='hello',

        bash_command='echo "Airflow DAG Running Successfully"'

    )