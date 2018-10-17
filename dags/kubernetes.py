import datetime as dt

from airflow import DAG
from airflow.contrib.operators import kubernetes_pod_operator


dag = DAG(
    dag_id="kubernetes_dag",
    schedule_interval="30 7 * * *",
    default_args={
        "owner": "airflow",
        "start_date": dt.datetime(2018, 10, 1),
        "depends_on_past": True,
        "email_on_failure": True,
        "email": "airflow_errors@myorganisation.com",
    },
)

# Only name, namespace, image, and task_id are required to create a
# KubernetesPodOperator. In Cloud Composer, currently the operator defaults
# to using the config file found at `/home/airflow/composer_kube_config if
# no `config_file` parameter is specified. By default it will contain the
# credentials for Cloud Composer's Google Kubernetes Engine cluster that is
# created upon environment creation.
kubernetes_min_pod = kubernetes_pod_operator.KubernetesPodOperator(
        # The ID specified for the task.
        task_id='pod-ex-minimum',
        # Name of task you want to run, used to generate Pod ID.
        name='pod-ex-minimum',
        # The namespace to run within Kubernetes, default namespace is
        # `default`. There is the potential for the resource starvation of
        # Airflow workers and scheduler within the Cloud Composer environment,
        # the recommended solution is to increase the amount of nodes in order
        # to satisfy the computing requirements. Alternatively, launching pods
        # into a custom namespace will stop fighting over resources.
        namespace='default',
        # Docker image specified. Defaults to hub.docker.com, but any fully
        # qualified URLs will point to a custom repository. Supports private
        # gcr.io images if the Composer Environment is under the same
        # project-id as the gcr.io images.
        image='hello-world',
        dag=dag
)
