from airflow import DAG

from airflow.operators.bash import BashOperator

from datetime import datetime


with DAG(

    dag_id='crypto_pipeline',

    start_date=datetime(2026,1,1),

    schedule=None,

    catchup=False

) as dag:


    run_publisher = BashOperator(

        task_id='run_publisher',

        bash_command="""

        python C:/Users/ADMIN/Github_repos/dataflow_practice_ramya/publisher/crypto_publisher.py

        """

    )


    run_dataflow = BashOperator(

        task_id='run_dataflow',

        bash_command="""

        python C:/Users/ADMIN/Github_repos/dataflow_practice_ramya/Dataflow/crypto_dataflow.py

        """

    )


    run_publisher >> run_dataflow