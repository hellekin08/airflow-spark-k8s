with DAG(
    dag_id="spark_on_k8s_airflow",
    start_date=timezone.datetime(2025, 8, 24, tzinfo=timezone.utc),
    schedule="@daily",
    catchup=False,
    params={
        "spark_namespace": "spark-operator",
        "spark_image": "docker.io/library/spark:4.0.0s",
        "executor_instances": 1,
        "app_suffix": "",          
        "main_file": "local:///opt/spark/examples/src/main/python/pi.py",
    },
    template_searchpath=[TEMPLATE_DIR],
) as dag:
    submit_spark = SparkKubernetesOperator(
        task_id="submit_spark",
        name="submit-spark",                     # keep constant
        application_file=APP_FILE,               # YAML is templated
        namespace="{{ params.spark_namespace }}",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
    )

    # Must match YAML's metadata.name template
    rendered_app_name = (
        "{{ dag.dag_id | replace('_','-') }}-{{ ts_nodash | lower }}"
        "{{ ('-' ~ params.app_suffix) if params.app_suffix else '' }}"
    )

    wait_for_spark = SparkKubernetesSensor(
        task_id="wait_for_spark",
        application_name=rendered_app_name,
        namespace="{{ params.spark_namespace }}",
        kubernetes_conn_id="kubernetes_default",
        attach_log=True,
        mode="reschedule",
        poke_interval=15,
        timeout=60 * 60,
    )

    submit_spark >> wait_for_spark
