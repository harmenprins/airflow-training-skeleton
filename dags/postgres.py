import airflow
from airflow import DAG
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from pendulum import Pendulum

args = {"owner": "Harmen", "start_date": airflow.utils.dates.days_ago(3)}
dag = DAG(dag_id="exercise4",default_args=args)

host = '178.62.227.89'
username = "gdd"
database = "gdd"
pw = "supergeheim123abc!"

pgsl_to_gcs = PostgresToGoogleCloudStorageOperator(
    task_id="p2gcs",
    sql="SELECT * FROM land_registry_price_paid_uk WHERE transfer_date = '{{ ds }}'",
    bucket="europe-west1-training-airfl-596abff0-bucket",
    filename="data/output",
    postgres_conn_id="training_postgres",
    dag=dag
)
