from datetime import datetime
import logging

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from ingest_data import ingest_data_postgres
from properties import *

LOGGER = logging.getLogger(__name__)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2019, 1, 1),
    "end_date": datetime(2019, 12, 3),
    "depends_on_past": True,
    "retries": 1
}


with DAG(
    dag_id="data_ingestion_fhv_postgres",
    schedule_interval="0 6 2 * *",
    default_args=default_args,
    catchup=True,
    max_active_runs=2,
    tags=['de-export-postgres']
) as dag:

    dataset_file_name = "fhv_tripdata_{{ logical_date.strftime(\'%Y-%m\') }}.csv"
    dataset_url = "https://nyc-tlc.s3.amazonaws.com/trip+data/" + dataset_file_name
    zone_file_url = "https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"
    zone_file_name = zone_file_url.split('/')[-1]



    download_dataset = BashOperator(
        task_id="download_dataset",
        bash_command=f'curl -sSL {dataset_url} > {path_to_local_home}/{dataset_file_name}'
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
