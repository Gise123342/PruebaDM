#Exportamos datos a postgres

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

from sqlalchemy import create_engine, types
import pandas as pd

@data_exporter
def export_data(data, *args, **kwargs):
   
    engine = create_engine(
        "postgresql+psycopg2://root:root@warehouse:5432/postgres"
    )

    results = {}

    for table_name, df in data.items():
        if not isinstance(df, pd.DataFrame) or df.empty:
            print(f"Tabla {table_name} vacía, se omite exportación")
            continue

        print(f"Exportando {len(df)} filas a tabla {table_name}...")

        # Exportar con payload como JSONB
        df.to_sql(
            table_name,
            engine,
            if_exists="append",  # para no borrar la tabla en cada carga
            index=False,
            dtype={
                "payload": types.JSON,  # PostgreSQL interpretará como JSONB
            }
        )

        results[table_name] = {
            "rows": len(df),
            "columns": len(df.columns),
            "status": "success"
        }

    print("Exportación completa")
    return results
