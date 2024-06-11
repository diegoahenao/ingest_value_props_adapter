from prefect import task, get_run_logger
from pydrive2.drive import GoogleDrive
from google.cloud import storage
import io


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
    logger.info(f"Transferir {file_name} de Google Drive a Google Cloud Storage...")
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