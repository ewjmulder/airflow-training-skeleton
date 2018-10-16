import datetime as dt

from airflow import DAG
from godatadriven.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator
from airflow.contrib.operators.dataproc_operator import (
  DataprocClusterCreateOperator,
  DataprocClusterDeleteOperator,
  DataProcPySparkOperator
)

dag = DAG(
    dag_id="training_dag",
    schedule_interval="30 7 * * *",
    default_args={
        "owner": "airflow",
        "start_date": dt.datetime(2018, 10, 1),
        "depends_on_past": True,
        "email_on_failure": True,
        "email": "emulder@bol.com",
    },
)


copy_task = PostgresToGoogleCloudStorageOperator(
    task_id="copy_postgres_to_gcs",
    postgres_conn_id="training_postgres",
    sql="SELECT * FROM land_registry_price_paid_uk WHERE transfer_date = '{{ ds }}'",
    bucket="airflow-training",
    filename="exports/{{ ds }}/land_registry_price.json",
    dag=dag
)

dataproc_create_cluster = DataprocClusterCreateOperator(
    task_id="create_cluster",
    cluster_name="training_cluster",
    project="airflowbolcom-1d3b3a0049ce78da",
    num_workers=2,
    zone="europe-west1",
    dag=dag
)

copy_task >> dataproc_create_cluster

