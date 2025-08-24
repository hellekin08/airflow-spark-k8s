# airflow/dags/spark.py
from datetime import timedelta
import os

from airflow import DAG
from airflow.models import Variable
from airflow.utils import timezone
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor

# --- Config (override via Airflow Variables if you like) ---
SPARK_NAMESPACE = Variable.get("SPARK_NAMESPACE", default_var="spark-operator")
SPARK_APP_NAME = Variable.get("SPARK_APP_NAME", default_var="osds-spark-app")
# Path to the folder that contains your YAML (resolved relative to this DAG file)
_THIS_DIR = os.path.dirname(__file__)
TEMPLATE_DIR = os.path.join(_THIS_DIR, "spark-example")  # contains spark-app.yaml
APP_FILE = "spark-app.yaml"  # file lives inside TEMPLATE_DIR

default_args = {
    "owner": "Howdy",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="spark_on_k8s_airflow",
    start_date=timezone.datetime(2025, 8, 24, tz="UTC"),  # static, tz-aware
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    # Make Jinja lookups find the YAML reliably regardless of deployment path
    template_searchpath=[TEMPLATE_DIR],
    tags=["spark", "kubernetes", "spark-operator"],
) as dag:

    # Submit the SparkApplication CR (defined in spark-app.yaml)
    spark_submit = SparkKubernetesOperator(
        task_id="submit_spark",
        application_file=APP_FILE,              # resolved via template_searchpath
        namespace=SPARK_NAMESPACE,
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,                     # IMPORTANT: no XCom sidecar for SparkApplication
    )

    # Wait until the SparkApplication reaches COMPLETED/FAILED and stream driver logs
    wait_for_spark = SparkKubernetesSensor(
        task_id="wait_for_spark",
        application_name=SPARK_APP_NAME,        # must match metadata.name in spark-app.yaml
        namespace=SPARK_NAMESPACE,
        kubernetes_conn_id="kubernetes_default",
        attach_log=True,                        # tail driver logs in Airflow
        poke_interval=15,                       # seconds
        timeout=60 * 60,                        # 1 hour (tune for your jobs)
        mode="reschedule",                      # free the worker slot between pokes
    )

    spark_submit >> wait_for_spark
