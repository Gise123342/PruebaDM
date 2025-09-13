if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

@transformer
def transform_data(data, *args, **kwargs):
    # Paso 1: facturas principales
    invoices = data.get("Invoice", [])
    df_invoices = pd.json_normalize(invoices, sep="_")
    
    # Paso 2: líneas de cada factura
    df_lines = pd.json_normalize(
        invoices,
        record_path=["Line"],
        meta=["Id", "DocNumber"],  # claves de la factura padre
        sep="_"
    )

    # Retornamos ambos dataframes
    return {
        "invoices": df_invoices,
        "invoice_lines": df_lines
    }
