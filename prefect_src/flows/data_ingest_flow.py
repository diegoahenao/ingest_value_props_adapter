from prefect import flow
from tasks.auth_task import (
    authenticate_drive,
    authenticate_gcs
)
from typing import List
import os
from tasks.get_data import (
    get_files_from_drive_to_gcs,
    read_lines_from_gcs,
    batch_lines
)

files_to_process: List[str] = ["taps.json", "prints.json", "pays.csv"]
google_drive_folder_id: str = os.environ.get("GOOGLE_DRIVE_FOLDER_ID")
bucket_name: str = os.environ.get("BUCKET_NAME")
batch_size: int = int(os.environ.get("BATCH_SIZE"))

@flow(name="ingest_value_props_adapter")
def main_flow() -> None:
    """Correr el flujo del ingest_value_props_adapter"""
    drive = authenticate_drive()
    storage_client = authenticate_gcs()
    for file_name in files_to_process:
        #get_files_from_drive_to_gcs(drive, storage_client, google_drive_folder_id, file_name, bucket_name)
        lines = read_lines_from_gcs(bucket_name, file_name, storage_client)
        batches = batch_lines(lines, batch_size)

if __name__ == "__main__":
    main_flow()
