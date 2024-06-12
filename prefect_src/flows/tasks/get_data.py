from prefect import task, get_run_logger
from pydrive2.drive import GoogleDrive
from google.cloud import storage
import io
from typing import List, Generator
import os
import json
import csv


@task
def get_files_from_drive_to_gcs(drive: GoogleDrive, storage_client: storage.Client, google_drive_folder_id: str, file_name: str, bucket_name: str) -> None:
    """Transferir archivos de Google Drive a Google Cloud Storage.
    
    Args:
        drive (GoogleDrive): Un objeto de Google Drive para autenticación.
        google_drive_folder_id (str): ID del directorio de Google Drive.
        file_name (str): Nombre del archivo para transferir.
        bucket_name (str): Nombre del bucket de Google Cloud Storage donde el archivo será cargado
    """
    logger = get_run_logger()
    logger.info(f"Transfiriendo {file_name} de Google Drive a Google Cloud Storage...")
    try:
        file_list = drive.ListFile({'q': f"'{google_drive_folder_id}' in parents and title='{file_name}'"}).GetList()
        if file_list:
           file = file_list[0]
           file_content = io.BytesIO(file.GetContentString(mimetype='application/octet-stream').encode('utf-8'))
           bucket = storage_client.bucket(bucket_name)
           blob = bucket.blob(file_name)
           blob.upload_from_file(file_content, rewind=True)
           logger.info(f"Transferencia exitosa del archivo {file_name} al bucket {bucket_name}")
        else:
           logger.warning(f"El archivo {file_name} no fue encontrado en el folder de Google Drive")
    except Exception as e:
        logger.error(f"Error al transferir {file_name} desde Google Drive al Google Cloud Storage: {e}")

@task
def read_lines_from_gcs(bucket_name: str, file_name: str, storage_client: storage.Client) -> Generator[List[str], None, None]:
    """Leer cada linea de un archivo desde Google Cloud Storage
    Args:
        bucket_name (str): Nombre del bucket de Google Cloud Storage.
        file_name (str): Nombre del archivo.
        storage_client (storage.Client): Objeto storage.Client para conectarse a Google Cloud Storage.

    Yields:
        List[str]: Lista con las lineas del archivo leido.
    """
    logger = get_run_logger()
    logger.info(f"Leyendo archivo {file_name} desde el bucket {bucket_name}...")
    json_objects = []
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        cleaned_filename = os.path.splitext(file_name)[0]
        with blob.open("rt") as file:
            if cleaned_filename in ["taps", "prints"]:
                for line in file:
                    try:
                        json_obj = json.loads(line.strip())
                        yield {
                            "day": json_obj.get("day"),
                            "position": json_obj.get("event_data", {}).get("position"),
                            "value": json_obj.get("event_data", {}).get("value_prop"),
                            "user_id": json_obj.get("user_id")
                        }
                    except json.JSONDecodeError as e:
                        logger.error(f"Error al decodificar JSON en línea: {line.strip()} - Error: {e}")
            elif cleaned_filename == "pays":
                reader = csv.reader(file)
                for line in reader:
                    try:
                        line_data = {
                            "pay_date": line[0],
                            "total": line[1],
                            "user_id": line[2],
                            "value_prop": line[3]
                        }
                        yield line_data
                    except IndexError as e:
                        logger.error(f"Error al procesar linea CSV: {line} - Error: {e}")
                    
    except Exception as e:
        logger.error(f"Error leyendo el archivo {file_name} desde el GCS: {e}")
        raise