# airflow/dags/spark-example/spark.py
from datetime import timedelta
import os

from airflow import DAG
from airflow.utils import timezone
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_DIR = _THIS_DIR
APP_FILE = "spark-app.yaml"

default_args = {
    "owner": "Howdy",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

def _extract_spark_app_name(ti, params):
    """
    Normalize whatever the operator pushed into a plain string name.
    Falls back to params['spark_app_name'] if needed.
    """
    x = ti.xcom_pull(task_ids="submit_spark")
    name = None
    if isinstance(x, str):
        name = x
    elif isinstance(x, dict):
        # most providers return the created CR body
        name = (x.get("metadata") or {}).get("name")
    ti.xcom_push(key="spark_app_name", value=name or params["spark_app_name"])

with DAG(
    dag_id="spark_on_k8s_airflow",
    start_date=timezone.datetime(2025, 8, 24, tzinfo=timezone.utc),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    template_searchpath=[TEMPLATE_DIR],
    params={
        "spark_namespace": "spark-operator",
        "spark_image": "docker.io/library/spark:4.0.0",
        "executor_instances": 1,
        "app_suffix": "",
        "main_file": "local:///opt/spark/examples/src/main/python/pi.py",
        "spark_app_name": "spark-on-k8s-airflow",
    },
    tags=["spark", "kubernetes", "spark-operator"],
) as dag:

    submit_spark = SparkKubernetesOperator(
        task_id="submit_spark",
        application_file=APP_FILE,               # templated via template_searchpath
        namespace="{{ params.spark_namespace }}",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,                       # ensure the created name/body is captured
    )

    get_spark_app_name = PythonOperator(
        task_id="get_spark_app_name",
        python_callable=_extract_spark_app_name,
    )

    wait_for_spark = SparkKubernetesSensor(
        task_id="wait_for_spark",
        # read the exact name the operator created
        application_name="{{ ti.xcom_pull(task_ids='get_spark_app_name', key='spark_app_name') }}",
        namespace="{{ params.spark_namespace }}",
        kubernetes_conn_id="kubernetes_default",
        attach_log=True,
        mode="reschedule",
        poke_interval=15,
        timeout=60 * 60,
    )

    submit_spark >> get_spark_app_name >> wait_for_spark
