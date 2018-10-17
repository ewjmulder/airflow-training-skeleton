import datetime as dt
import tempfile

from airflow import DAG
from godatadriven.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataprocClusterDeleteOperator,
    DataProcPySparkOperator
)
from airflow.utils.trigger_rule import TriggerRule
from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

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
    source_objects=["average_prices/{{ ds }}/transfer_date={{ ds }}/*.parquet"],
    destination_project_dataset_table="airflowbolcom-1d3b3a0049ce78da.airflowtraining.avgp_{{ ds_nodash }}",
    source_format="parquet",
    write_disposition="WRITE_TRUNCATE",
    dag=dag
)

dataproc_delete_cluster >> gcs_to_bq


class HttpToGcsOperator(BaseOperator):
    """
    Calls an endpoint on an HTTP system to execute an action

    :param http_conn_id: The connection to run the operator against
    :type http_conn_id: string
    :param endpoint: The relative part of the full url. (templated)
    :type endpoint: string
    :param gcs_path: The path of the GCS to store the result
    :type gcs_path: string
    """

    template_fields = ('endpoint', 'gcs_path')
    template_ext = ('.json',)
    ui_color = '#f4a460'

    @apply_defaults
    def __init__(self,
                 http_conn_id,
                 endpoint,
                 gcs_path,
                 *args, **kwargs):
        super(HttpToGcsOperator, self).__init__(*args, **kwargs)
        self.http_conn_id = http_conn_id
        self.endpoint = endpoint
        self.gcs_path = gcs_path

    def execute(self, context):
        http = HttpHook(method='GET', http_conn_id=self.http_conn_id)
        response = http.run(self.endpoint)
        print("HTTP response: " + response.text)

        with tempfile.TemporaryFile() as tmp_file:
            tmp_file.write(response.content)
            tmp_file.flush()

        gcs = GoogleCloudStorageHook()
        gcs.upload(bucket="airflow-training", object=self.gcs_path,
                   filename=tmp_file.name, mime_type="application/json")


http_to_gcs = HttpToGcsOperator(
    task_id="http_to_gcs",
    http_conn_id="currency_converter",
    endpoint="/airflow-training-transform-valutas?date={{ ds }}&from=GBP&to=EUR",
    gcs_path="currency-exchange/{{ ds }}/",
    dag=dag
)
