#!/usr/bin/env python
# coding: utf-8

import argparse
from pathlib import Path

from time import time
from typing import NamedTuple, List

import pandas as pd
from rich.console import Console
from sqlalchemy import create_engine

from src.utils.downloader import download

console = Console()

# Create a named tuple containing url of csv file and name of the table
CSVFile = NamedTuple("CSV_URL", [("url", str), ("table_name", str)])


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_names = params.table_name
    urls = params.url

    # Create destination directory
    dest_dir = Path('resources/data/')
    dest_dir.mkdir(exist_ok=True, parents=True)

    downloaded_files = download([urls], dest_dir)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(downloaded_files[0], iterator=True, chunksize=100000)
    with console.status("[bold green]Loading data to database...") as status:
        for i, df in enumerate(df_iter):
            t_start = time()

            if i == 0:
                status.update(f"[bold green]Created table {table_name}")
                df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
            df.to_sql(name=table_name, con=engine, if_exists='append')
            t_end = time()

            status.update(f"Inserted {i + 1} chunk in {t_end - t_start:.2f} seconds")


if __name__ == '__main__':
    csv_files = ["https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv",
                 "https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"]

    table_names = ["yellow_taxi_trips", "taxi_zone_lookup"]

    parser = argparse.ArgumentParser(description="Ingest CSV data to Postgres")
    parser.add_argument("--user", default="root", help="user name for postgres")
    parser.add_argument('--password', default="root", help='password for postgres')
    parser.add_argument("--host", default="localhost", help="host for postgres")
    parser.add_argument("--port", default="5432", help="port for postgres")
    parser.add_argument("--db", default="ny_taxi", help="database name for postgres")
    parser.add_argument("--url", nargs="+", default=csv_files, help="List of csv files")
    parser.add_argument("--table_names", nargs="+", default=table_names, help="name of the table where we will write the results to")

    args = parser.parse_args()

    main(args)
