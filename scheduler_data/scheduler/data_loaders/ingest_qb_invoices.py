# librerías
import requests
import time
import pandas as pd
from sqlalchemy import create_engine
from mage_ai.data_preparation.shared.secrets import get_secret_value
from datetime import datetime, timedelta
import json

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


# función para jalar datos de la API
def _fetch_qb_data(realm_id, access_token, query, base_url, minor_version):
    if not base_url or not minor_version:
        raise ValueError("Se requiere una URL base y el minor_version")
    if not realm_id or not access_token:
        raise ValueError("Se requiere un realm_id y un access_token")

    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/json',
        'Content-Type': 'text/plain'
    }
    params = {
        'query': query,
        'minorversion': minor_version
    }
    url = f"{base_url.rstrip('/')}/v3/company/{realm_id}/query"

    # Reintentos
    max_retries = 5
    for i in range(max_retries):
        try:
            print(f'Request a la API: {url}\nQuery: {query}')
            response = requests.get(url, headers=headers, params=params, timeout=60)

            if response.status_code == 200:  # éxito
                data = response.json()
                print('Datos recibidos de la API correctamente')
                return data

            elif response.status_code in [429, 500, 502, 503, 504]:  # errores temporales
                print(f"Error {response.status_code}, reintentando ({i+1}/{max_retries})...")
                time.sleep(2 ** i)  # backoff exponencial
                continue
            else:
                response.raise_for_status()

        except requests.exceptions.RequestException as e:
            print(f"Error en la pull de la API: {e}")
            time.sleep(2 ** i)

    raise Exception(f"Request falló después de {max_retries} reintentos")


# obtiene un access_token fresco
def get_access_token():
    client_id = get_secret_value('qb_client_id')
    client_secret = get_secret_value('qb_client_secret')
    refresh_token = get_secret_value('qb_refresh_token')

    url = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    auth = (client_id, client_secret)
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token
    }

    response = requests.post(url, headers=headers, data=data, auth=auth)
    response.raise_for_status()
    token_data = response.json()
    return token_data['access_token']


# paginación para facturas
def fetch_invoices_with_pagination(
    realm_id, access_token, base_url, minor_version,
    chunk_start_str, chunk_end_str, page_size=1000
):
    invoices = []
    start_position = 1

    while True:
        query = (
            f"SELECT * FROM Invoice "
            f"WHERE Metadata.LastUpdatedTime >= '{chunk_start_str}' "
            f"AND Metadata.LastUpdatedTime < '{chunk_end_str}' "
            f"STARTPOSITION {start_position} MAXRESULTS {page_size}"
        )
        data = _fetch_qb_data(realm_id, access_token, query, base_url, minor_version)
        batch = data.get("QueryResponse", {}).get("Invoice", [])
        if not batch:
            break

        invoices.extend(batch)
        start_position += page_size

        if len(batch) < page_size:  # última página
            break

    return invoices


# procesa chunks de backfill
def process_backfill_chunks(realm_id, access_token, base_url, minor_version,
                            fecha_inicio, fecha_fin, chunk_days=7):
    start_dt = datetime.fromisoformat(fecha_inicio.replace('Z', '+00:00'))
    end_dt = datetime.fromisoformat(fecha_fin.replace('Z', '+00:00'))

    all_invoices = []
    processing_log = []
    current_date = start_dt
    chunk_number = 1

    print(f"Iniciando backfill de Invoice desde {fecha_inicio} hasta {fecha_fin}")
    print(f"Segmentación: chunks de {chunk_days} días")

    while current_date < end_dt:
        chunk_end = min(current_date + timedelta(days=chunk_days), end_dt)
        chunk_start_str = current_date.strftime('%Y-%m-%dT%H:%M:%S-00:00')
        chunk_end_str = chunk_end.strftime('%Y-%m-%dT%H:%M:%S-00:00')

        print(f"\n--- CHUNK {chunk_number} ---")
        print(f"Procesando: {chunk_start_str} a {chunk_end_str}")

        chunk_start_time = time.time()
        try:
            invoices_in_chunk = fetch_invoices_with_pagination(
                realm_id, access_token, base_url, minor_version,
                chunk_start_str, chunk_end_str
            )

            for invoice in invoices_in_chunk:
                invoice['_chunk_metadata'] = {
                    'chunk_number': chunk_number,
                    'chunk_start': chunk_start_str,
                    'chunk_end': chunk_end_str,
                    'processed_at': datetime.utcnow().isoformat()
                }
            all_invoices.extend(invoices_in_chunk)

            chunk_duration = time.time() - chunk_start_time
            log_entry = {
                "chunk_number": chunk_number,
                "fecha_inicio_chunk": chunk_start_str,
                "fecha_fin_chunk": chunk_end_str,
                "paginas_leidas": 1,
                "filas_procesadas": len(invoices_in_chunk),
                "duracion_segundos": round(chunk_duration, 2),
                "status": "success",
                "timestamp": datetime.utcnow().isoformat()
            }
            processing_log.append(log_entry)

            print(f"Chunk {chunk_number} completado:")
            print(f" - Invoices: {len(invoices_in_chunk)}")
            print(f" - Duración: {chunk_duration:.2f}s")

        except Exception as e:
            error_duration = time.time() - chunk_start_time
            log_entry = {
                "chunk_number": chunk_number,
                "fecha_inicio_chunk": chunk_start_str,
                "fecha_fin_chunk": chunk_end_str,
                "paginas_leidas": 0,
                "filas_procesadas": 0,
                "duracion_segundos": round(error_duration, 2),
                "status": "error",
                "error_message": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }
            processing_log.append(log_entry)
            print(f"Error en chunk {chunk_number}: {str(e)}")

        current_date = chunk_end
        chunk_number += 1
        time.sleep(1)  # pausa entre chunks

    total_invoices = len(all_invoices)
    successful_chunks = len([log for log in processing_log if log['status'] == 'success'])
    failed_chunks = len([log for log in processing_log if log['status'] == 'error'])
    total_duration = sum([log['duracion_segundos'] for log in processing_log])

    print(f"\n=== RESUMEN BACKFILL INVOICE ===")
    print(f"Total invoices procesados: {total_invoices}")
    print(f"Chunks exitosos: {successful_chunks}")
    print(f"Chunks fallidos: {failed_chunks}")
    print(f"Duración total: {total_duration:.2f}s")

    return {
        "QueryResponse": {"Invoice": all_invoices},
        "_processing_log": processing_log,
        "_backfill_summary": {
            "total_invoices": total_invoices,
            "successful_chunks": successful_chunks,
            "failed_chunks": failed_chunks,
            "total_duration": total_duration,
            "fecha_inicio": fecha_inicio,
            "fecha_fin": fecha_fin
        }
    }


# loader de datos de la tubería Mage
@data_loader
def load_data(*args, **kwargs):
    realm_id = get_secret_value('qb_realm_id')
    access_token = get_access_token()
    minor_version = 75
    base_url = 'https://sandbox-quickbooks.api.intuit.com'

    fecha_inicio = kwargs.get('fecha_inicio')
    fecha_fin = kwargs.get('fecha_fin')

    if fecha_inicio and fecha_fin:
        print("Modo: BACKFILL con parámetros de fecha para Invoice")
        chunk_days = kwargs.get('chunk_days', 7)

        data = process_backfill_chunks(
            realm_id, access_token, base_url, minor_version,
            fecha_inicio, fecha_fin, chunk_days
        )
        data['_extraction_metadata'] = {
            'extraction_type': 'backfill',
            'entity': 'Invoice',
            'fecha_inicio': fecha_inicio,
            'fecha_fin': fecha_fin,
            'chunk_days': chunk_days,
            'extracted_at': datetime.utcnow().isoformat()
        }
    else:
        print("Modo: EXTRACCIÓN COMPLETA de Invoice (sin filtros de fecha)")
        query = 'SELECT * FROM Invoice'
        data = _fetch_qb_data(realm_id, access_token, query, base_url, minor_version)
        data['_extraction_metadata'] = {
            'extraction_type': 'full',
            'entity': 'Invoice',
            'extracted_at': datetime.utcnow().isoformat()
        }

    return data


@test
def test_output(output, *args) -> None:
    assert output is not None, 'El output está vacío'

    if '_processing_log' in output:
        assert len(output['_processing_log']) > 0, 'No hay logs de procesamiento'
        print(f"Chunks procesados: {len(output['_processing_log'])}")

    query_response = output.get("QueryResponse", {})
    invoices = query_response.get("Invoice", [])
    print(f"Total invoices extraídos: {len(invoices)}")
