from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime, timedelta
import os, sys
from airflow import DAG
import pendulum

# start_date = datetime(2024, 11, 20)

start_date = pendulum.datetime(2024, 11, 20, tz="UTC")

default_args = {
    'owner': 'airflow',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('tmdb_dag', default_args=default_args, schedule='@daily', catchup=False) as dag:
    t1 = SSHOperator(
    task_id="task1",
    command="/opt/spark/bin/spark-submit --packages io.delta:delta-spark_2.12:3.1.0 /dataops/crew.py",
    conn_timeout=300, cmd_timeout=300,
    ssh_conn_id='spark_ssh_conn')

    t1
