#libreria para hacer peticiones http
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


#funcion para jalar datos de la API (anonima, no aparece)
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

    #Reintentos
    max_retries = 5
    for i in range(max_retries):
        try:
            print(f'Request a la API: {url}\nQuery: {query}')
            response = requests.get(url, headers=headers, params=params, timeout=60)

            #respuestas exitosas
            if response.status_code == 200:
                data = response.json()
                print('Datos recividos de la API correctamente')
                return data

            # deteccion de errores
            elif response.status_code in [429, 500, 502, 503, 504]:
                print(f"Error {response.status_code}, reintentando ({i+1}/{max_retries})...")
                time.sleep(2 ** i)  # backoff exponencial
                continue

            else:
                #si hay un error levanta la excepcion
                response.raise_for_status()

         #Error que nos da de la excepcion
        except requests.exceptions.RequestException as e:
            print(f"Error en la pull de la API: {e}")
            time.sleep(2 ** i)

    raise Exception(f"Request falló después de {max_retries} reintentos")

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

    #access_token fresco
    return token_data['access_token']

def fetch_items_with_pagination(realm_id, access_token, base_url, minor_version, chunk_start_str, chunk_end_str, page_size=1000):
    items = []
    start_position = 1

    while True:
        query = (
            f"SELECT * FROM Item "
            f"WHERE Metadata.LastUpdatedTime >= '{chunk_start_str}' "
            f"AND Metadata.LastUpdatedTime < '{chunk_end_str}' "
            f"STARTPOSITION {start_position} MAXRESULTS {page_size}"
        )

        data = _fetch_qb_data(realm_id, access_token, query, base_url, minor_version)
        batch = data.get("QueryResponse", {}).get("Item", [])

        if not batch:
            break

        items.extend(batch)
        start_position += page_size

        # si devuelve menos del límite → no hay más páginas
        if len(batch) < page_size:
            break

    return items


#  Procesar backfill con segmentación para Item
def process_backfill_chunks(realm_id, access_token, base_url, minor_version, fecha_inicio, fecha_fin, chunk_days=7):
    """
    Procesa backfill dividido en chunks por fechas para Item
    """
    start_dt = datetime.fromisoformat(fecha_inicio.replace('Z', '+00:00'))
    end_dt = datetime.fromisoformat(fecha_fin.replace('Z', '+00:00'))
    
    all_items = []
    processing_log = []
    
    current_date = start_dt
    chunk_number = 1
    
    print(f"Iniciando backfill de Item desde {fecha_inicio} hasta {fecha_fin}")
    print(f"Segmentación: chunks de {chunk_days} días")
    
    while current_date < end_dt:
        chunk_end = min(current_date + timedelta(days=chunk_days), end_dt)
        
        # Formatear fechas para QuickBooks (sin microsegundos)
        chunk_start_str = current_date.strftime('%Y-%m-%dT%H:%M:%S-00:00')
        chunk_end_str = chunk_end.strftime('%Y-%m-%dT%H:%M:%S-00:00')
        
        print(f"\n--- CHUNK {chunk_number} ---")
        print(f"Procesando: {chunk_start_str} a {chunk_end_str}")
        
        chunk_start_time = time.time()
        
        try:
            # Query con filtro de fechas - QB usa LastUpdatedTime para Item
            # Ahora con paginación
            items_in_chunk = fetch_items_with_pagination(
                realm_id, access_token, base_url, minor_version, chunk_start_str, chunk_end_str
                )

            
            # Agregar metadata del chunk a cada item
            for item in items_in_chunk:
                item['_chunk_metadata'] = {
                    'chunk_number': chunk_number,
                    'chunk_start': chunk_start_str,
                    'chunk_end': chunk_end_str,
                    'processed_at': datetime.utcnow().isoformat()
                }
            
            all_items.extend(items_in_chunk)
            
            chunk_duration = time.time() - chunk_start_time
            
            # REGISTRAR CADA TRAMO
            log_entry = {
                "chunk_number": chunk_number,
                "fecha_inicio_chunk": chunk_start_str,
                "fecha_fin_chunk": chunk_end_str,
                "paginas_leidas": 1,  # QB no usa paginación tradicional, usar 1
                "filas_procesadas": len(items_in_chunk),
                "duracion_segundos": round(chunk_duration, 2),
                "status": "success",
                "timestamp": datetime.utcnow().isoformat()
            }
            
            processing_log.append(log_entry)
            
            print(f"Chunk {chunk_number} completado:")
            print(f"   - Items: {len(items_in_chunk)}")
            print(f"   - Duración: {chunk_duration:.2f}s")
            
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
            # Continuar con siguiente chunk en lugar de fallar todo
        
        # Siguiente chunk
        current_date = chunk_end
        chunk_number += 1
        
        # Pausa entre chunks para no sobrecargar API
        time.sleep(1)  # 1 segundo entre chunks
    
    # RESUMEN FINAL
    total_items = len(all_items)
    successful_chunks = len([log for log in processing_log if log['status'] == 'success'])
    failed_chunks = len([log for log in processing_log if log['status'] == 'error'])
    total_duration = sum([log['duracion_segundos'] for log in processing_log])
    
    print(f"\n=== RESUMEN BACKFILL ITEM ===")
    print(f"Total items procesados: {total_items}")
    print(f"Chunks exitosos: {successful_chunks}")
    print(f"Chunks fallidos: {failed_chunks}")
    print(f"Duración total: {total_duration:.2f}s")
    
    return {
        "QueryResponse": {
            "Item": all_items
        },
        "_processing_log": processing_log,
        "_backfill_summary": {
            "total_items": total_items,
            "successful_chunks": successful_chunks,
            "failed_chunks": failed_chunks,
            "total_duration": total_duration,
            "fecha_inicio": fecha_inicio,
            "fecha_fin": fecha_fin
        }
    }

#funcion de carga de datos dentro del mage 
@data_loader
def load_data(*args, **kwargs):
    
    realm_id = get_secret_value('qb_realm_id')
    access_token = get_access_token()
    minor_version = 75 
    base_url = 'https://sandbox-quickbooks.api.intuit.com' 
    
    # VERIFICAR SI ES BACKFILL CON PARÁMETROS
    fecha_inicio = kwargs.get('fecha_inicio')
    fecha_fin = kwargs.get('fecha_fin')
    
    if fecha_inicio and fecha_fin:
        # MODO BACKFILL CON SEGMENTACIÓN
        print("Modo: BACKFILL con parámetros de fecha para Item")
        chunk_days = kwargs.get('chunk_days', 7)  # Default: 1 semana por chunk
        
        data = process_backfill_chunks(
            realm_id, 
            access_token, 
            base_url, 
            minor_version, 
            fecha_inicio, 
            fecha_fin, 
            chunk_days
        )
        
        # Agregar metadata global
        data['_extraction_metadata'] = {
            'extraction_type': 'backfill',
            'entity': 'Item',
            'fecha_inicio': fecha_inicio,
            'fecha_fin': fecha_fin,
            'chunk_days': chunk_days,
            'extracted_at': datetime.utcnow().isoformat()
        }
        
    else:
        # MODO TRADICIONAL - extraer todos los items
        print("Modo: EXTRACCIÓN COMPLETA de Item (sin filtros de fecha)")
        query = 'SELECT * FROM Item' 
        data = _fetch_qb_data(realm_id, access_token, query, base_url, minor_version)
        
        # Agregar metadata para mantener consistencia
        data['_extraction_metadata'] = {
            'extraction_type': 'full',
            'entity': 'Item',
            'extracted_at': datetime.utcnow().isoformat()
        }
    
    return data


@test
def test_output(output, *args) -> None:
    assert output is not None, 'El output está vacío'
    
    # Test específico para backfill
    if '_processing_log' in output:
        assert len(output['_processing_log']) > 0, 'No hay logs de procesamiento'
        print(f"Chunks procesados: {len(output['_processing_log'])}")
    
    query_response = output.get("QueryResponse", {})
    items = query_response.get("Item", [])
    print(f"Total items extraídos: {len(items)}")