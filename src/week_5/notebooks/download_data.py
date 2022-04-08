"""
A rudimentary URL downloader (like wget or curl) to demonstrate Rich progress bars.
"""

import subprocess
from dataclasses import InitVar, dataclass
import os.path
from pathlib import Path
from concurrent.futures import as_completed, ThreadPoolExecutor
import signal
from functools import partial
from threading import Event
from typing import Iterable, Optional, Tuple
from urllib.request import urlopen
import gzip

from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TaskID,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

progress = Progress(
    TextColumn("[bold blue]{task.fields[filename]}", justify="right"),
    BarColumn(bar_width=None),
    "[progress.percentage]{task.percentage:>3.1f}%",
    "•",
    DownloadColumn(),
    "•",
    TransferSpeedColumn(),
    "•",
    TimeRemainingColumn(),
)

@dataclass
class FileDownload:
    """A file download."""
    destination_dir: InitVar[Path]
    color: str
    year: int
    month: int

    def __post_init__(self, destination_dir: Path):
        self.filename = f"{self.color}_tripdata_{year}-{month:02d}.csv"
        self.url = f"https://s3.amazonaws.com/nyc-tlc/trip+data/{self.filename}"
        self.dest_file = destination_dir/f"{self.color}/{year}/{month}/{self.filename}"
        self.compressed_file = destination_dir/f"{self.color}/{year}/{month}/{self.filename}.gz"

    def is_file_already_downloaded(self) -> bool:
        return self.compressed_file.exists()


done_event = Event()


def handle_sigint(signum, frame):
    done_event.set()


signal.signal(signal.SIGINT, handle_sigint)

def gzip_file(file_download: FileDownload) -> None:
    # run subprocess to gzip the file
    subprocess.run(["gzip", file_download.dest_file])

def copy_url(task_id: TaskID, file_download: FileDownload) -> None:
    """Copy data from a url to a local file."""
    if not file_download.is_file_already_downloaded():
        response = urlopen(file_download.url)
        # This will break if the response doesn't contain content length
        progress.update(task_id, total=int(response.info()["Content-length"]))
        with open(file_download.dest_file, "wb") as dest_file:
            progress.start_task(task_id)
            for data in iter(partial(response.read, 32768), b""):
                dest_file.write(data)
                progress.update(task_id, advance=len(data))
                if done_event.is_set():
                    return
        # gzip the file
        gzip_file(file_download)

        # Remove the uncompressed file
        os.remove(file_download.dest_file)
    else:
        progress.update(task_id, total=0)
        progress.update(task_id, advance=0)
    

def download(file_downloads: Iterable[FileDownload], dest_dir: Path):
    """Download multuple files to the given directory."""

    with progress:
        with ThreadPoolExecutor(max_workers=4) as pool:
            for file_download in file_downloads:
                filename = file_download.url.split("/")[-1]
                file_download.dest_file.parent.mkdir(parents=True, exist_ok=True)
                task_id = progress.add_task("download", filename=filename, start=False)
                pool.submit(copy_url, task_id, file_download)


if __name__ == "__main__":
    
    destination_dir = Path("data/raw/").resolve()
    files_to_download: list[FileDownload] = []
    for year in (2020, 2021):
        for month in range(1, 13):
            file_download = FileDownload(destination_dir, "green", year, month)
            files_to_download.append(file_download)

            file_download = FileDownload(destination_dir, "yellow", year, month)
            files_to_download.append(file_download)

    download(files_to_download, destination_dir)
