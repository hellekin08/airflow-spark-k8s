https://medium.com/@howdyservices9/executing-spark-operator-using-airflow-on-kubernetes-d5c17de7d376

```
helm repo add apache-airflow https://airflow.apache.org
helm upgrade --install airflow apache-airflow/airflow --namespace airflow --create-namespace

helm show values apache-airflow/airflow > values.yaml

helm upgrade --install airflow apache-airflow/airflow `
  --namespace airflow --create-namespace `
  --values values.yaml


kubectl port-forward svc/airflow-api-server 8080:8080 --namespace airflow
```




