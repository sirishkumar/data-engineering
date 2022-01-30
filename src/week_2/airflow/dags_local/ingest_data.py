#!/usr/bin/env python
# coding: utf-8

import os
import argparse
from functools import partial
from pathlib import Path

from time import time
from typing import NamedTuple, List
from urllib.request import urlopen

import pandas as pd
from rich.console import Console
from sqlalchemy import create_engine

console = Console()

from rich.traceback import install
install(show_locals=True)


def download(url: str, dest_dir: Path) -> Path:
    """Download multuple files to the given directory."""

    filename = url.split("/")[-1]
    dest_path = dest_dir/filename
    response = urlopen(url)
    console.log(f"Requesting {url}")
    with open(dest_path, "wb") as dest_file:
        # Read data in chunks and terminate when the end of the file is reached
        for data in iter(partial(response.read, 32768), "b"):
            if not data:
                break

            console.log(f"Write {len(data)} bytes to file {dest_path}")
            dest_file.write(data)

    return dest_path


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    time_columns = params.time_columns

    # Create destination directory
    dest_dir = Path('resources/data/')
    dest_dir.mkdir(exist_ok=True, parents=True)

    file_path = download(url, dest_dir)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(file_path, iterator=True, chunksize=100000)
    with console.status("[bold green]Loading data to database...") as status:
        for i, df in enumerate(df_iter):
            t_start = time()

            if i == 0:
                status.update(f"[bold green]Created table {table_name}")
                df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

            for col in time_columns:
                df[col] = pd.to_datetime(df[col])

            df.to_sql(name=table_name, con=engine, if_exists='append')
            t_end = time()

            status.update(f"Inserted {i + 1} chunk in {t_end - t_start:.2f} seconds")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Ingest CSV data to Postgres")
    parser.add_argument("--user", default="root", help="user name for postgres")
    parser.add_argument('--password', default="root", help='password for postgres')
    parser.add_argument("--host", default="localhost", help="host for postgres")
    parser.add_argument("--port", default="5432", help="port for postgres")
    parser.add_argument("--db", default="ny_taxi", help="database name for postgres")
    parser.add_argument("--url", required=True, type=str, help="List of csv files")
    parser.add_argument("--table_name", required=True, type=str, help="name of the table where we will write the results to")
    parser.add_argument("--time_columns", nargs="+", type=str, help="columns that should be converted to datetime")

    args = parser.parse_args()

    main(args)
