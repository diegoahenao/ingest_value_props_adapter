from prefect import task, get_run_logger
from pydrive2.drive import GoogleDrive
from pydrive2.auth import GoogleAuth, ServiceAccountCredentials
import os
import json
from google.cloud import storage

GOOGLE_SERVICE_ACCOUNT_JSON: str = json.loads(os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON"))

@task
def authenticate_drive() -> GoogleDrive:
    """Autenticarse en Google Drive usando una cuenta de servicio y regresar un objeto GoogleDrive."""
    logger = get_run_logger()
    scope = ["https://www.googleapis.com/auth/drive.readonly"]
    gauth = GoogleAuth()
    gauth.credentials = ServiceAccountCredentials.from_json_keyfile_dict(GOOGLE_SERVICE_ACCOUNT_JSON, scope)
    drive = GoogleDrive(gauth)
    logger.info(f"Generado objeto GoogleDrive para autenticación")
    return drive

@task
def authenticate_gcs() -> storage.Client:
    """Autenticarse en Google Cloud Storage y regresar un objeto storage.Client"""
    logger = get_run_logger()
    storage_client = storage.Client.from_service_account_info(GOOGLE_SERVICE_ACCOUNT_JSON)
    logger.info(f"Generado objeto storage.Client para autenticación")
    return storage_client
