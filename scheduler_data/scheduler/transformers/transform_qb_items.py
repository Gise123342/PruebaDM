#funcion de transformacion de datos 
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd
from datetime import datetime, timezone
import json

@transformer
def transform(data, *args, **kwargs):
 
    #Utilizamos el formato especificado en el deber 

    # Datos de la API
    query_response = data.get("QueryResponse", {})
    items = query_response.get("Item", [])

    print(f"Número de items encontrados: {len(items)}")

    if not items:
        print("No hay items para procesar")
        return {"qb_item": pd.DataFrame()}

    # Extraer metadatos 
    extract_window_start_utc = kwargs.get("extract_window_start_utc", datetime.now(timezone.utc).isoformat())
    extract_window_end_utc = kwargs.get("extract_window_end_utc", datetime.now(timezone.utc).isoformat())
    page_number = kwargs.get("page_number", 1)
    page_size = kwargs.get("page_size", len(items))
    request_payload = kwargs.get("request_payload", {})

    df_items = pd.DataFrame([
        {
            #variables determinadas en el pdf de deber
            "id": item.get("Id"),
            "payload": json.dumps(item, default=str),
            "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
            "extract_window_start_utc": extract_window_start_utc,
            "extract_window_end_utc": extract_window_end_utc,
            "page_number": page_number,
            "page_size": page_size,
            "request_payload": json.dumps(request_payload, default=str),
        }
        for item in items
    ])

    print(f"Items transformados: {len(df_items)} filas, {len(df_items.columns)} columnas")

    return {"qb_item": df_items}


@test
def test_output(output, *args) -> None:
    """
    Test para validar que se creó la tabla qb_item correctamente
    """
    assert output is not None, "El output está vacío"
    assert "qb_item" in output, "qb_item no encontrado en output"
    df = output["qb_item"]
    assert "id" in df.columns, "Columna id no encontrada"
    assert "payload" in df.columns, "Columna payload no encontrada"
    print(f"Test passed - qb_item shape: {df.shape}")
