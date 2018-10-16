import datetime as dt

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from godatadriven.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator


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
