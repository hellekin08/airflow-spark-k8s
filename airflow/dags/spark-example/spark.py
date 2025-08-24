# airflow/dags/spark-example/spark.py
from datetime import timedelta
import os
from airflow import DAG
from airflow.models import Variable
from airflow.utils import timezone
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor

SPARK_NAMESPACE = Variable.get("SPARK_NAMESPACE", default_var="spark-operator")
_THIS_DIR = os.path.dirname(__file__)
TEMPLATE_DIR = os.path.join(_THIS_DIR, "spark-example")
APP_FILE = "spark-app.yaml"

default_args = {"owner": "Howdy", "retries": 0, "retry_delay": timedelta(minutes=5)}  # retries=0 while debugging

APP_NAME_TPL = "{{ dag.dag_id }}-{{ ts_nodash | lower }}"   # unique name per run

with DAG(
    dag_id="spark_on_k8s_airflow",
    start_date=timezone.datetime(2025, 8, 24, tzinfo=timezone.utc),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    template_searchpath=[TEMPLATE_DIR],
    tags=["spark", "kubernetes", "spark-operator"],
) as dag:

    submit_spark = SparkKubernetesOperator(
        task_id="submit_spark",
        application_file=APP_FILE,
        namespace=SPARK_NAMESPACE,
        kubernetes_conn_id="kubernetes_default",
        params={"app_name": APP_NAME_TPL, "spark_namespace": SPARK_NAMESPACE},
        do_xcom_push=True,   # safe here; it’s regular XCom, no sidecar
    )

    wait_for_spark = SparkKubernetesSensor(
        task_id="wait_for_spark",
        application_name="{{ ti.xcom_pull(task_ids='submit_spark')['metadata']['name'] }}",
        namespace=SPARK_NAMESPACE,
        kubernetes_conn_id="kubernetes_default",
        attach_log=True,
        poke_interval=15,
        timeout=60 * 60,
        mode="reschedule",
    )

    submit_spark >> wait_for_spark
