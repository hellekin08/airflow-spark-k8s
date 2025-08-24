from datetime import timedelta
import os
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.utils import timezone  # ✅ use this instead of days_ago
from airflow.models import Variable

default_args = {
    'owner': 'Howdy',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'spark_on_k8s_airflow',
    # Option A (dynamic): start yesterday from "now" in UTC
    start_date=timezone.utcnow() - timedelta(days=1),
    # Option B (static, recommended): uncomment the next line and set a fixed date
    # start_date=timezone.datetime(2025, 8, 23, tz="UTC"),
    catchup=False,
    schedule="@daily",                  # (equivalent shorthand)
    template_searchpath='/opt/airflow/dags/repo/dags/spark8s/',
    default_args=default_args,
)

spark_k8s_task = SparkKubernetesOperator(
    task_id='n-spark-on-k8s-airflow',
    trigger_rule="all_success",
    depends_on_past=False,
    retries=0,
    application_file='spark-k8s.yaml',
    namespace="spark-operator",
    kubernetes_conn_id="kubernetes_default",
    do_xcom_push=True,
    dag=dag,
)

spark_k8s_task
