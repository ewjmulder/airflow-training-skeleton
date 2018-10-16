import datetime as dt

from airflow import DAG
from godatadriven.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataprocClusterDeleteOperator,
    DataProcPySparkOperator
)
from airflow.utils.trigger_rule import TriggerRule

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
    cluster_name="analyse-pricing-{{ ds }}",
    project_id="airflowbolcom-1d3b3a0049ce78da",
    num_workers=2,
    zone="europe-west4-a",
    dag=dag
)

copy_task >> dataproc_create_cluster

compute_aggregates = DataProcPySparkOperator(
    task_id="compute_aggregates",
    main="gs://europe-west1-training-airfl-3665e525-bucket/other/build_statistics_simple.py",
    cluster_name="analyse-pricing-{{ ds }}",
    arguments=["{{ ds }}"],
    dag=dag
)

dataproc_create_cluster >> compute_aggregates

dataproc_delete_cluster = DataprocClusterDeleteOperator(
    task_id="delete_cluster",
    cluster_name="analyse-pricing-{{ ds }}",
    project_id="airflowbolcom-1d3b3a0049ce78da",
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag
)

compute_aggregates >> dataproc_delete_cluster

gcs_to_bq = GoogleCloudStorageToBigQueryOperator(
    task_id="bcs_to_bq",
    bucket="airflow-training",
    source_objects="average_prices/{{ ds }}/transfer_date={{ ds }}/*.parquet",
    destination_project_dataset_table="airflowbolcom-1d3b3a0049ce78da.airflowtraining.avgp_{{ ds_nodash }}",
    source_format="parquet",
    write_disposition="WRITE_TRUNCATE",
    dag=dag
)

dataproc_delete_cluster >> gcs_to_bq
