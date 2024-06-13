from prefect import flow
from tasks.auth_task import authenticate_drive
from typing import List
import os
from tasks.get_data import (
    get_files_from_drive,
    read_lines_from_gcs,
    batch_lines
)

files_to_process: List[str] = ["taps.json", "prints.json", "pays.csv"]
google_drive_folder_id: str = os.environ.get("GOOGLE_DRIVE_FOLDER_ID")
batch_size: int = int(os.environ.get("BATCH_SIZE"))

@flow(name="ingest_value_props_adapter")
def main_flow() -> None:
    """Correr el flujo del ingest_value_props_adapter"""
    drive = authenticate_drive()
    for file_name in files_to_process:
        file_content = get_files_from_drive(drive, google_drive_folder_id, file_name)
        lines = read_lines_from_gcs(file_name, file_content)
        batches = batch_lines(lines, batch_size)
        print(batches)

if __name__ == "__main__":
    main_flow()
