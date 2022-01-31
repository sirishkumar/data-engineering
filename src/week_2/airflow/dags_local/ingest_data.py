#!/usr/bin/env python
# coding: utf-8

from functools import partial
from pathlib import Path

from time import time
from typing import NamedTuple, List, Optional
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


def ingest_data_postgres(csv_file_path: Path,
                         user: str,
                         password: str,
                         host: str, 
                         port: int, 
                         db: str, 
                         table_name: str, 
                         time_columns: Optional[List[str]] = None):
    
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # check if table already exists
    if table_name in engine.table_names():
        console.log(f"Table {table_name} already exists. Skipping...")
        return

    df_iter = pd.read_csv(csv_file_path, iterator=True, chunksize=100000)
    console.log("[bold green]Loading data to database...")
    for i, df in enumerate(df_iter):
        t_start = time()

        if i == 0:
            console.log(f"[bold green]Created/Replace table {table_name}")
            df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

        if time_columns:
            for col in time_columns:
                df[col] = pd.to_datetime(df[col])

        df.to_sql(name=table_name, con=engine, if_exists='append')
        t_end = time()

        console.log(f"Inserted {i + 1} chunk in {t_end - t_start:.2f} seconds")

