from prefect import flow
from tasks.auth_task import (
    authenticate_drive,
    authenticate_gcs
)

@flow(name="ingest_value_props_adapter")
def main_flow() -> None:
    """Correr el flujo del ingest_value_props_adapter"""
    drive = authenticate_drive()
    storage_client = authenticate_gcs()

if __name__ == "__main__":
    main_flow()
