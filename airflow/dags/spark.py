from datetime import timedelta
import os
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

default_args = {
    'owner': 'Howdy',
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'spark_on_k8s_airflow',
    start_date=days_ago(1),
    catchup=False,
    schedule_interval=timedelta(days=1),
    template_searchpath='/opt/airflow/dags/repo/dags/spark8s/'
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
    dag=dag
)

spark_k8s_task