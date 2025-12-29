from airflow import DAG
from airflow.providers.apache.kafka.sensors.kafka import KafkaSensor
from airflow.operators.python import PythonOperator
from datetime import datetime

def process_message(**context):
    message = context["ti"].xcom_pull(task_ids="wait_for_kafka")
    print("Received Kafka message:", message)

with DAG(
    dag_id="kafka_triggered_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Triggered only by Kafka
    catchup=False,
    tags=["kafka"],
) as dag:

    wait_for_kafka = KafkaSensor(
        task_id="wait_for_kafka",
        kafka_config_id="kafka_default",  # Airflow Connection
        topics=["my-topic"],
        poll_timeout=5,
        max_messages=1,
    )

    process = PythonOperator(
        task_id="process_message",
        python_callable=process_message,
        provide_context=True,
    )

    wait_for_kafka >> process
