from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime, timedelta
import os, sys
from airflow import DAG
import pendulum

from airflow.models import Variable

MINIO_ACCESS_KEY = Variable.get("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = Variable.get("MINIO_SECRET_KEY")


# start_date = datetime(2024, 11, 20)

start_date = pendulum.datetime(2024, 11, 20, tz="UTC")

default_args = {
    'owner': 'airflow',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('tmdb_dag2', default_args=default_args, schedule='@daily', catchup=False) as dag:
    t1 = SSHOperator(
    task_id="task1",
     command=f"""
    export MINIO_ACCESS_KEY={MINIO_ACCESS_KEY}
    export MINIO_SECRET_KEY={MINIO_SECRET_KEY}

    /opt/spark/bin/spark-submit \
      --packages io.delta:delta-spark_2.12:3.1.0 \
      /dataops/crew.py
    """,
    conn_timeout=300, cmd_timeout=300,
    ssh_conn_id='spark_ssh_conn')

    t1
















pull_and_run = SSHOperator(
    task_id="pull_and_run_tmdb_spark",
    ssh_conn_id="spark_ssh_conn",
    conn_timeout=300,
    cmd_timeout=600,
    command=r"""
        set -euo pipefail

        DEST="/dataops/TMDB_DELTA-1"
        BRANCH="{{ params.branch }}"

        # --- GitHub token ile güvenli erişim ---
        set +x
        TOKEN="{{ params.github_token }}"
        REPO_URL="https://${TOKEN}@github.com/erkansirin78/TMDB_DELTA-1.git"
        set -x

        # --- Repo sync ---
        if [ ! -d "${DEST}/.git" ]; then
            rm -rf "${DEST}"
            git clone --depth 1 --branch "${BRANCH}" "${REPO_URL}" "${DEST}"
        else
            cd "${DEST}"
            set +x
            git remote set-url origin "${REPO_URL}"
            set -x

            git fetch origin "${BRANCH}"
            git reset --hard "origin/${BRANCH}"
            git clean -fd
        fi

        # --- MinIO env ---
        export MINIO_ACCESS_KEY="{{ params.minio_access_key }}"
        export MINIO_SECRET_KEY="{{ params.minio_secret_key }}"
        export MINIO_ENDPOINT="{{ params.minio_endpoint }}"

        # --- Spark job ---
        /opt/spark/bin/spark-submit \
          --packages io.delta:delta-spark_2.12:3.1.0 \
          dataops/crew.py
    """,
    params={
        "github_token": github_token,          # Airflow Variable / Secret
        "branch": "main",
        "minio_access_key": MINIO_ACCESS_KEY,
        "minio_secret_key": MINIO_SECRET_KEY,
        "minio_endpoint": "http://minio:9000",
    },
)
