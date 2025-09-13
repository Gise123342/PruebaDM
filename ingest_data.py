import pandas as pd
#from sqlalchemy import create_engine
import argparse
import os
from sqlalchemy import create_engine


#motor = create_engine('postgresql://root@localhost:5432/ny_taxi')

#datos = pd.read_csv("https://s3.amazonaws.com/nys-tlc/trip+data/yellow_tripdata_2021-01.csv")

#datos.head()

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    
    #url pasada para que se genere el gz cuando se ejecute el archivo 
    if url.endswith('.csv.gz'):
        csv_name = 'raw_data.csv.gz'
    else:
        csv_name = 'raw_data.csv'


    #os.system(f'wget {url} -O {csv_name}')  
    
    import requests
    print(f"Descargando {url} ...")
    r = requests.get(url)
    with open(csv_name, "wb") as f:
        f.write(r.content)
    print(f"Archivo guardado como {csv_name}")

    url_conn = f'postgresql://{user}:{password}@{host}:{port}/{db}'
    
    engine = create_engine(url_conn)

    data_raw_iter = pd.read_csv(csv_name, iterator=True, chunksize=1000000)

    data_raw = next(data_raw_iter)

    print(data_raw.head())

if __name__  == '__main__':

    parser = argparse.ArgumentParser(description='Ingesta de datos de CSV a Postgres')

    parser.add_argument('--user', required=True, help='Username para la bd en postgres')
    parser.add_argument('--password', required=True, help='Password para la bd en postgres')
    parser.add_argument('--host', required=True, help='Host para la bd en postgres')
    parser.add_argument('--port', required=True, help='Port para la bd en postgres')
    parser.add_argument('--db', required=True, help='DB para la bd en postgres')
    parser.add_argument('--table_name', required=True, help='Table para la bd en postgres')
    parser.add_argument('--url', required=True, help='URL para la bd en postgres')

    args = parser.parse_args()

    main(args)