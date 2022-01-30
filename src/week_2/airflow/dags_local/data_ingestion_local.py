from datetime import datetime
import os
import logging
from pathlib import Path

from google.cloud import storage
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq
from pydata_google_auth import default

from ingest_data import ingest_data_postgres

LOGGER = logging.getLogger(__name__)

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DATABASE = os.getenv("PG_DATABASE")


path_to_local_home = Path(os.environ.get("AIRFLOW_HOME", "/opt/airflow/")).resolve()


def format_to_parquet(src_file: str) -> None:
    if not src_file.endswith('.csv'):
        LOGGER.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))


def upload_to_gcs(bucket: str, object_name: str, local_file_path: Path) -> None:
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file_path: file path to upload
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file_path)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2019, 1, 1),
    "end_date": datetime(2020, 1, 3),
    "depends_on_past": True,
    "retries": 1
}


with DAG(
    dag_id="data_ingestion_fhv_postgres",
    schedule_interval="0 6 2 * *",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['dtd-de-postgres']
) as dag:

    dataset_file_name = "fhv_tripdata_{{ logical_date.strftime(\'%Y-%m\') }}.csv"
    dataset_url = "https://nyc-tlc.s3.amazonaws.com/trip+data/" + dataset_file_name
    zone_file_url = "https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"
    zone_file_name = zone_file_url.split('/')[-1]


    # 1. Download dataset from S3
    zone_file_path = path_to_local_home/zone_file_name

    download_dataset = BashOperator(
        task_id="download_dataset",
        bash_command=f'curl -sSL {dataset_url} > {path_to_local_home}/{dataset_file_name}'
    )

    download_zone_lookup = BashOperator(
        task_id="download_zone_lookup",
        bash_command=f"curl -sS {zone_file_url} > {zone_file_path}"
    )

    ingest_dataset = PythonOperator(
        task_id="ingest_data",
        python_callable=ingest_data_postgres,
        op_kwargs=dict(
                csv_file_path=f"{path_to_local_home}/{dataset_file_name}",
                user=PG_USER,
                password=PG_PASSWORD,
                host=PG_HOST,
                port=PG_PORT,
                db=PG_DATABASE,
                table_name="fhv_trips_{{logical_date.strftime(\'%Y_%m\')}}",
                time_columns=["pickup_datetime", "dropoff_datetime"],
        )
    )

    delete_dataset = BashOperator(
        task_id="delete_dataset",
        bash_command=f"rm {path_to_local_home}/{dataset_file_name}"
    )

    ingest_zone_lookup = PythonOperator(
        task_id="ingest_update_zone_lookup",
        python_callable=ingest_data_postgres,
        op_kwargs=dict(
                csv_file_path=f"{path_to_local_home}/{zone_file_name}",
                user=PG_USER,
                password=PG_PASSWORD,
                host=PG_HOST,
                port=PG_PORT,
                db=PG_DATABASE,
                table_name="zone_lookup_trips",
        )
    )

    delete_zone_lookup = BashOperator(
        task_id="delete_zone_lookup",
        bash_command=f"rm {path_to_local_home}/{zone_file_name}"
    )

    # format_to_parquet_dataset = PythonOperator(
    #     task_id="format_to_parquet_dataset",
    #     python_callable=format_to_parquet,
    #     op_kwargs={"src_file": f"{dataset_file_path}"}
    # )

    # format_to_parquet_zone_lookup = PythonOperator(
    #     task_id="format_to_parquet_zone_lookup",
    #     python_callable=format_to_parquet,
    #     op_kwargs={"src_file": f"{zone_file_path}"}
    # )


# Run tasks in parallel
download_dataset >> ingest_dataset >> delete_dataset
download_zone_lookup >> ingest_zone_lookup >> delete_zone_lookup