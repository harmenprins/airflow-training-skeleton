import airflow
from airflow import DAG
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from pendulum import Pendulum

args = {"owner": "Harmen", "start_date": airflow.utils.dates.days_ago(3)}
dag = DAG(dag_id="exercise4",default_args=args)
project_id = "airflowbolcom-6b5317463050ef21"

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

from airflow.contrib.operators.dataproc_operator import *

dataproc_create_cluster = DataprocClusterCreateOperator(
    task_id="create_dataproc",
    cluster_name="analyse-pricing-{{ ds }}",
    project_id=project_id,
    num_workers=2,
    zone="europe-west4-a",
    dag=dag
)

compute_aggregates = DataProcPySparkOperator(
    task_id="compute_aggregates",
    main="gs://europe-west1-training-airfl-596abff0-bucket/build_statistics.py",
    cluster_name="analyse-pricing-{{ ds }}",
    arguments=["{{ ds }}"],
    dag=dag
)

dataproc_delete_cluster = DataprocClusterDeleteOperator(
    task_id="delete_dataproc",
    cluster_name="analyse-pricing-{{ ds }}",
    project_id=project_id,
    dag=dag
)

pgsl_to_gcs >> dataproc_create_cluster >> compute_aggregates >> dataproc_delete_cluster
