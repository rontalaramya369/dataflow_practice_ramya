from airflow import DAG

from airflow.operators.bash import BashOperator

from airflow.operators.empty import EmptyOperator

from datetime import datetime



default_args = {

    "owner":"airflow",

    "depends_on_past":False

}



with DAG(

    dag_id="crypto_pipeline",

    default_args=default_args,

    start_date=datetime(2026,6,20),

    schedule=None,

    catchup=False,

    tags=["streaming","pubsub","dataflow","bigquery"]

) as dag:



    start=EmptyOperator(

        task_id="start"

    )



    run_publisher=BashOperator(

        task_id="run_publisher",

        bash_command="""

        echo "Starting Publisher"

        nohup python /opt/airflow/publisher/crypto_publisher.py > /tmp/publisher.log 2>&1 &

        sleep 20

        echo "Publisher Started"

        """

    )



    run_dataflow=BashOperator(

        task_id="run_dataflow",

        bash_command="""

        echo "Starting Dataflow"

        nohup python /opt/airflow/Dataflow/crypto_dataflow.py > /tmp/dataflow.log 2>&1 &

        echo "Dataflow Started"

        """

    )



    start >> run_publisher >> run_dataflow