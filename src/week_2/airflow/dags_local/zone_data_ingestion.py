from datetime import datetime
import logging

from airflow import DAG
from google.cloud import storage
import pyarrow.csv as pv
import pyarrow.parquet as pq
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from ingest_data import ingest_data_postgres
from properties import *

LOGGER = logging.getLogger(__name__)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 1, 1),
    "retries": 1
}


with DAG(
    dag_id="data_ingestion_zone_data",
    default_args=default_args,
    schedule_interval=None,
    tags=["de-zone-data"]
) as dag:

    zone_file_url = "https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"
    zone_file_name = zone_file_url.split('/')[-1]
    zone_file_path = path_to_local_home/zone_file_name

    download_zone_lookup = BashOperator(
        task_id="download_zone_lookup",
        bash_command=f"curl -sS {zone_file_url} > {zone_file_path}"
    )

    ingest_zone_lookup = PythonOperator(
        task_id="ingest_zone_lookup",
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
    
download_zone_lookup >> ingest_zone_lookup >> delete_zone_lookup