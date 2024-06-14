from prefect import task, get_run_logger
from pydrive2.drive import GoogleDrive
from pydrive2.auth import GoogleAuth, ServiceAccountCredentials
import os
import json
from google.cloud import storage
import httpx

GOOGLE_SERVICE_ACCOUNT_JSON: str = json.loads(os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON"))
TOKEN_URL = os.environ.get('TOKEN_URL')

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

@task
def get_token(api_key: str) -> str:
  """Generar un token temporal usando una API Key
  
  Args:
    api_key (str): Api key.
  
  Returns:
    str: Token temporal.
  """
  auth_url = TOKEN_URL
  try:
    response = httpx.post(auth_url, json={"api_key": api_key})
    response.raise_for_status()
    token = response.json().get("access_token")
    if not token:
      raise ValueError('No se puedo obtener el token temporal en la respuesta')
    return token
  except httpx.RequestError as e:
    print(f'Un error ocurrió mientras se solicitaba el token: {e}')
    raise
  except httpx.HTTPStatusError as e:
    print(f'Error en la respuesta: {e.response.status_code}')
    raise
