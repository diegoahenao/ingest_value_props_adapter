import os
from prefect import task, get_run_logger
from typing import List, Any
import httpx

TIMEOUT_MINUTES = int(os.environ.get('TIMEOUT_MINUTES'))

@task
def send_batch_to_api(batch: List[List[Any]], api_url: str, file_name: str, token: str) -> None:
    """Enviar batches a la API
    
    Args:
        batch (List[List[Any]]): Batch para enviar a la API.
        api_url (str): API url.
    """
    logger = get_run_logger()
    try:
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        with httpx.Client(timeout=TIMEOUT_MINUTES) as client:
            cleaned_filename = os.path.splitext(file_name)[0]
            response = client.post(f'{api_url}/{cleaned_filename}', json=batch, headers=headers)
            response.raise_for_status()
            logger.info(f'Batch de {len(batch)} registros enviado exitosamente.')
    except httpx.RequestError as e:
        logger.error(f'Un error ocurrio mientras se realizaba la petición {e.request.url!r}: {e}')
    except httpx.HTTPStatusError as e:
        logger.error(f'Respuesta del error {e.response.status_code} mientras hacia la petición {e.request.url!r}: {e}')