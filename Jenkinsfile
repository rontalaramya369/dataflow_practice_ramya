pipeline {

    agent any

    stages {

        stage('Checkout') {

            steps {

                checkout scm

            }

        }

        stage('Verify Files') {

            steps {

                bat 'dir'

            }

        }

        stage('Deploy DAG') {

            steps {

                bat '''

                copy dags\\crypto_dag.py C:\\Users\\ADMIN\\Desktop\\airflow-project\\dags

                '''

            }

        }

    }

}