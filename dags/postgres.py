from tempfile import NamedTemporaryFile

import airflow
from airflow import DAG
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator
from airflow.hooks.http_hook import HttpHook
from airflow.utils.trigger_rule import TriggerRule

args = {"owner": "Harmen", "start_date": airflow.utils.dates.days_ago(3)}
dag = DAG(dag_id="exercise4", default_args=args)
project_id = "airflowbolcom-6b5317463050ef21"

host = '178.62.227.89'
username = "gdd"
database = "gdd"
pw = "supergeheim123abc!"

pgsl_to_gcs = PostgresToGoogleCloudStorageOperator(
    task_id="p2gcs",
    sql="SELECT * FROM land_registry_price_paid_uk WHERE transfer_date = '{{ ds }}'",
    bucket="europe-west1-training-airfl-596abff0-bucket",
    filename="data/output_{{ ds_nodash }}.json",
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


class HttpToGcsOperator(BaseOperator):
    """
    Calls an endpoint on an HTTP system to execute an action

    :param http_conn_id: The connection to run the operator against
    :type http_conn_id: string
    :param endpoint: The relative part of the full url. (templated)
    :type endpoint: string
    :param gcs_path: The path of the GCS to store the result
    :type gcs_path: string
    """

    template_fields = ("endpoint", "gcs_path")
    template_ext = ()
    ui_color = "#f4a460"

    @apply_defaults
    def __init__(
            self,
            endpoint,
            gcs_bucket,
            gcs_path,
            method="GET",
            http_conn_id="http_default",
            gcs_conn_id="google_cloud_default",
            *args,
            **kwargs
    ):
        super(HttpToGcsOperator, self).__init__(*args, **kwargs)
        self.http_conn_id = http_conn_id
        self.method = method
        self.endpoint = endpoint
        self.gcs_bucket = gcs_bucket
        self.gcs_path = gcs_path
        self.gcs_conn_id = gcs_conn_id

    def execute(self, context):
        http = HttpHook(self.method, http_conn_id=self.http_conn_id)

        self.log.info("Calling HTTP method")
        response = http.run(self.endpoint)

        with NamedTemporaryFile() as tmp_file_handle:
            tmp_file_handle.write(response.content)
            tmp_file_handle.flush()

            hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=self.gcs_conn_id)
            hook.upload(
                bucket=self.gcs_bucket,
                object=self.gcs_path,
                filename=tmp_file_handle.name,
            )


http2gcs = HttpToGcsOperator(
    endpoint="date={{ ds }}&from=GBP&to=EUR",
    gcs_bucket="europe-west1-training-airfl-596abff0-bucket",
    gcs_path="currency/{{ ds }}-EUR.json",
    task_id="get_currency",
    project_id=project_id,
    dag=dag,
    http_conn_id='currency',
)

gcs2bq = GoogleCloudStorageToBigQueryOperator(
    task_id="gcs2bq",
    bucket="europe-west1-training-airfl-596abff0-bucket",
    source_objects=["/average_prices/"],
    destination_project_dataset_table="prices.prices${{ ds_nodash }}",
    source_format="PARQUET",
    write_disposition="WRITE_TRUNCATE",
    dag=dag
)

pgsl_to_gcs >> dataproc_create_cluster >> compute_aggregates >> dataproc_delete_cluster >> gcs2bq
http2gcs >> dataproc_create_cluster