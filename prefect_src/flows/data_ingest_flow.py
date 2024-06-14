from prefect import flow
from tasks.auth_task import (
    authenticate_drive,
    authenticate_gcs,
    get_token
)
from typing import List
import os
from tasks.get_data import (
    get_files_from_drive_to_gcs,
    read_lines_from_gcs,
    batch_lines
)
from tasks.post_data import send_batch_to_api

files_to_process: List[str] = ["pays.csv"]
google_drive_folder_id: str = os.environ.get("GOOGLE_DRIVE_FOLDER_ID")
bucket_name: str = os.environ.get("BUCKET_NAME")
batch_size: int = int(os.environ.get("BATCH_SIZE"))
api_url = os.environ.get('API_URL')
api_key = os.environ.get('API_KEY')

@flow(name="ingest_value_props_adapter")
def main_flow() -> None:
    """Correr el flujo del ingest_value_props_adapter"""
    drive = authenticate_drive()
    storage_client = authenticate_gcs()
    for file_name in files_to_process:
        get_files_from_drive_to_gcs(drive, storage_client, google_drive_folder_id, file_name, bucket_name)
        lines = read_lines_from_gcs(bucket_name, file_name, storage_client)
        batches = batch_lines(lines, batch_size)
        for batch in batches:
            token = get_token(api_key)
            send_batch_to_api(batch, api_url, file_name, token)

if __name__ == "__main__":
    main_flow()

