from prefect import task, get_run_logger
from pydrive2.drive import GoogleDrive
from google.cloud import storage
import io
from typing import List, Generator
import os
import json
import csv


@task
def get_files_from_drive(drive: GoogleDrive, google_drive_folder_id: str, file_name: str) -> None:
    """Obtener archivos desde Google Drive.
    
    Args:
        drive (GoogleDrive): Un objeto de Google Drive para autenticación.
        google_drive_folder_id (str): ID del directorio de Google Drive.
        file_name (str): Nombre del archivo para transferir.
    """
    logger = get_run_logger()
    logger.info(f"Obteniendo {file_name} de Google Drive...")
    try:
        file_list = drive.ListFile({'q': f"'{google_drive_folder_id}' in parents and title='{file_name}'"}).GetList()
        if file_list:
           file = file_list[0]
           file_content = io.BytesIO(file.GetContentString(mimetype='application/octet-stream').encode('utf-8'))
           logger.info(f"El archivo {file_name} fue encontrado")
        else:
           logger.warning(f"El archivo {file_name} no fue encontrado en el folder de Google Drive")
    except Exception as e:
        logger.error(f"Error al transferir {file_name} desde Google Drive al Google Cloud Storage: {e}")
    return file_content

@task
def read_lines_from_gcs(file_name: str, file_content: io.BytesIO) -> Generator[List[str], None, None]:
    """Leer cada linea de un archivo
    Args:
        file_name (str): Nombre del archivo.
        file_content (io.BytesIO): Contenido del archivo en un objeto BytesIO.

    Yields:
        List[str]: Lista con las lineas del archivo leido.
    """
    logger = get_run_logger()
    logger.info(f"Leyendo archivo {file_name}...")
    try:
        cleaned_filename = os.path.splitext(file_name)[0]
        file_content.seek(0)
        if cleaned_filename in ["taps", "prints"]:
            for line in file_content:
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
            file_content.seek(0)
            reader = csv.reader(io.TextIOWrapper(file_content, encoding='utf-8'))
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
        logger.error(f"Error leyendo el archivo {file_name}: {e}")
        raise

@task
def batch_lines(lines: Generator[List[str], None, None], batch_size: int) -> List[list[List[str]]]:
    """Agrupar las filas en batches de un tamaño especifico
    
    Args:
        lines (Generator[List[str], None, None]): Un generador de lineas.
        batch_size (int): Tamaño del batch.
    
    Returns:
        List[List[List[str]]]: Una lista de batches de filas.
    """
    logger = get_run_logger()
    logger.info(f"Batching {len(lines)} lineas en batches de tamaño {batch_size}.")
    batches = []
    batch = []
    try:
        for line in lines:
            batch.append(line)
            if len(batch) == batch_size:
                batches.append(batch)
                batch = []
        if batch:
            batches.append(batch)
    except Exception as e:
        logger.error(f"Error ocurrido mientras se agrupan las lineas: {e}")
        raise
    logger.info(f"Creados exitosamente {len(batches)} batches.")
    return batches