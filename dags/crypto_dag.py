from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime


default_args = {
    "owner": "airflow",
    "depends_on_past": False
}


with DAG(

    dag_id="crypto_pipeline",

    default_args=default_args,

    start_date=datetime(2026, 6, 20),

    schedule=None,

    catchup=False,

    tags=["crypto", "pubsub", "dataflow", "bigquery"]

) as dag:


    run_publisher = BashOperator(

        task_id="run_publisher",

        bash_command="""

        echo "Starting Crypto Publisher"

        python /opt/airflow/publisher/crypto_publisher.py

        """

    )


    run_dataflow = BashOperator(

        task_id="run_dataflow",

        bash_command="""

        echo "Starting Dataflow Streaming Job"

        python /opt/airflow/Dataflow/crypto_dataflow.py

        """

    )


    run_publisher >> run_dataflow