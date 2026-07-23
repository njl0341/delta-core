pipeline {
    agent any

    options {
        timestamps()
        disableConcurrentBuilds()
    }

    environment {
        VENV_DIR = '.venv-ci'
        PYTHONUNBUFFERED = '1'
    }

    stages {
        stage('Verify Python') {
            steps {
                sh '''
                    set -eux
                    command -v python3
                    python3 --version
                '''
            }
        }

        stage('Install Dependencies') {
            steps {
                sh '''
                    set -eux
                    python3 -m venv "$VENV_DIR"
                    . "$VENV_DIR/bin/activate"
                    python -m pip install --upgrade pip
                    pip install -e ".[dev]"
                '''
            }
        }

        stage('Lint') {
            steps {
                sh '''
                    set -eux
                    . "$VENV_DIR/bin/activate"
                    ruff check .
                '''
            }
        }

        stage('Run Tests') {
            steps {
                sh '''
                    set -eux
                    . "$VENV_DIR/bin/activate"
                    mkdir -p reports
                    pytest --junitxml=reports/pytest.xml
                '''
            }
        }
    }

    post {
        always {
            junit testResults: 'reports/pytest.xml', allowEmptyResults: true
            archiveArtifacts artifacts: 'reports/pytest.xml', allowEmptyArchive: true
        }
    }
}