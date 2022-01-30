import json
import pathlib

import airflow
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

dag = DAG(
    dag_id="rocket_launchs_dag",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval=None
)

download_launches_task = BashOperator(
    task_id="download_launches",
    bash_command="curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",
    dag=dag,
)

def _get_pictures():
    pathlib.Path("/tmp/pictures").mkdir(parents=True, exist_ok=True)

    with open("/tmp/launches.json") as launches_file:
        launches = json.load(launches_file)
        image_urls = [launch["image"] for launch in launches["results"]]
        for image_url in image_urls:
            try:
                response = requests.get(image_url)
                response.raise_for_status()
                image_filename = image_url.split("/")[-1]
                with open("/tmp/pictures/" + image_filename, "wb") as image_file:
                    image_file.write(response.content)
                print(f"Downloaded {image_filename}")
            except requests_exceptions.HTTPError as http_error:
                print(f"HTTP error occurred: {http_error}")
            except requests_exceptions.RequestException as request_exception:
                print(f"Request exception occurred: {request_exception}")


get_pictures = PythonOperator(
    task_id="get_pictures",
    python_callable=_get_pictures,
    dag=dag
)

notify_task = BashOperator(
    task_id="notify",
    bash_command='echo "There are $(ls /tmp/pictures | wc -l) pictures"',
    dag=dag
)


download_launches_task >> get_pictures >> notify_task
