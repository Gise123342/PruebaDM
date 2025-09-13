#Exportacion de datos a postgres
if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

from sqlalchemy import create_engine
import pandas as pd

@data_exporter
def export_data(data, *args, **kwargs):
    """
    Exporta los items transformados (qb_item) a PostgreSQL.
    """
    df_items = data["qb_item"]

    print(f"Datos originales - Items: {df_items.shape}")

    engine = create_engine(
        "postgresql+psycopg2://root:root@warehouse:5432/postgres"
    )

    try:
        print("Exportando tabla qb_item...")
        df_items.to_sql("qb_item", engine, if_exists="replace", index=False)

        print(" Datos de items exportados a Postgres correctamente")

        return {
            "rows": len(df_items),
            "columns": len(df_items.columns),
            "status": "success"
        }

    except Exception as e:
        print(f" Error durante la exportación: {str(e)}")
        raise e
