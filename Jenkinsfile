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

    }

}