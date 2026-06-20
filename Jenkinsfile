pipeline {

    agent any

    stages {

        stage('Checkout') {

            steps {

                checkout scm

            }

        }

        stage('Deploy DAG') {

            steps {

                bat 'copy dags\\crypto_dag.py C:\\Users\\ADMIN\\Desktop\\airflow-project\\dags'

            }

        }

        stage('Deploy Publisher') {

            steps {

                bat 'xcopy publisher C:\\Users\\ADMIN\\Desktop\\airflow-project\\publisher /E /Y'

            }

        }

        stage('Deploy Dataflow') {

            steps {

                bat 'xcopy Dataflow C:\\Users\\ADMIN\\Desktop\\airflow-project\\Dataflow /E /Y'

            }

        }

    }

}