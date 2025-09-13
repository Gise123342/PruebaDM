if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd
import json
from datetime import datetime

@transformer
def transform(data, *args, **kwargs):
   
    #Utilizamos el formato especificado en el deber 

    # Datos de la API
    query_response = data.get("QueryResponse", {})
    customers = query_response.get("Customer", [])

    # Metadata global
    ingested_at_utc = datetime.utcnow().isoformat()
    extract_window_start_utc = kwargs.get("extract_window_start_utc", ingested_at_utc)
    extract_window_end_utc = kwargs.get("extract_window_end_utc", ingested_at_utc)
    page_number = kwargs.get("page_number", 1)
    page_size = kwargs.get("page_size", len(customers))
    request_payload = kwargs.get("request_payload", {})

    rows = []
    for cust in customers:
        rows.append({
            "id": cust.get("Id"),
            "payload": json.dumps(cust),  # JSON completo de la entidad
            "ingested_at_utc": ingested_at_utc,
            "extract_window_start_utc": extract_window_start_utc,
            "extract_window_end_utc": extract_window_end_utc,
            "page_number": page_number,
            "page_size": page_size,
            "request_payload": json.dumps(request_payload),
        })

    df_customers = pd.DataFrame(rows)

    print(f"Transformados {len(df_customers)} customers en formato raw staging")

    return {
        "qb_customer": df_customers
    }


@test
def test_output(output, *args) -> None:
    assert output is not None, "El output está vacío"
    assert "qb_customer" in output, "No se encontró la tabla qb_customer en el output"
    print("Shape qb_customer:", output["qb_customer"].shape)