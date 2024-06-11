from prefect import flow
from tasks.auth_task import authenticate_drive

@flow(name="ingest_value_props_adapter")
def main_flow() -> None:
    """Correr el flujo del ingest_value_props_adapter"""
    drive = authenticate_drive()

if __name__ == "__main__":
    main_flow()
