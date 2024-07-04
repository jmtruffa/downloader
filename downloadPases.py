import os
import tempfile
import pandas as pd
import requests
from urllib3.exceptions import InsecureRequestWarning
import datetime
from dataBaseConn import DatabaseConnection
import sys
import sqlalchemy


def download():
    """
    Descarga el archivo de pases del BCRA
    """

    url = "https://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/Datapases.xlsx"

    # Create a temporary directory to store the downloaded file
    temp_dir = tempfile.mkdtemp()

    # File path for the downloaded XLSM file
    file_path = os.path.join(temp_dir, "data.xlsm")

    # Download the XLS file from the URL
    try:
        requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
        response = requests.get(url, verify=False)
        response.raise_for_status()  # Check if the request was successful
        with open(file_path, "wb") as file:
            file.write(response.content)
            current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"Archivo de pases descargado exitosamente a las {current_time}")            
    except requests.exceptions.RequestException as e:
        print(f"Error descargando el archivo. Hora:{current_time}. Error: {e}")
        return False

    return file_path


def parsePases(file_path = None):
    """
    Parsea el archivo de pases del BCRA que viene en format XLSX
    """
    # Read the specified range "A:B" from the first sheet
    data_df = pd.read_excel(file_path, sheet_name=0, usecols="A:V", skiprows=3)

    column_definitions = (
        "date",
        "ppRueda",
        "ppPlazo",
        "pptasa",
        "ppConcertacionesPublico",
        "ppConcertacionesPrivado",
        "ppConcertacionesOperadoBCRA",
        "ppStockPublico",
        "ppStockPrivado",
        "ppStockTotal",
        "paRueda",
        "paPlazo",
        "paptasa",
        "paConcertacionesPublico",
        "paConcertacionesPrivado",
        "paConcertacionesOperadoBCRA",
        "paStockPublico",
        "paStockPrivado",
        "paStockTotal",
        "tasaRefRIX",
        "montoOperado",
        "TPM"
    )

    data_df.columns = column_definitions

    data_df = data_df[pd.to_datetime(data_df['date'], format="%Y-%m-%d %H:%M:%S", errors='coerce').notna()]
    
    data_df['date'] = pd.to_datetime(data_df['date'], format="%Y-%m-%d %H:%M:%S").dt.date

    return data_df


def graba(df):
    """
    Graba el dataframe en la base de datos
    """
    # Create a connection to the database
    db = DatabaseConnection(db_type='postgresql', db_name=os.environ.get('POSTGRES_DB'))
    db.connect()

    # Check if the table exists
    table_exists_query = f"""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name = 'pases'
    """
    result = pd.read_sql(table_exists_query, db.conn)

    if result.empty:
        data_to_insert = df
    else:
        query = f'SELECT MAX(date) FROM "pases"'
        last_date = pd.read_sql(query, db.conn).iloc[0,0]
        # convert to datetime in order to compare to the date column in data_df
        last_date = pd.to_datetime(last_date)

        # Filter the data to insert
        data_to_insert = df[df['date'] > last_date]

    # Insert the data into the database
    if len(data_to_insert) == 0:
        print("No hay datos nuevos que grabar")
    else:
        print(f"Inserting {len(data_to_insert)} rows into pases Table")
        # use Date type for the 'date' column in the database to get rid of the time part
       # dtypeMap = {'date': sqlalchemy.types.Date, 'settleDate': sqlalchemy.types.Date}
        result = data_to_insert.to_sql(name = 'pases', con = db.engine, if_exists = 'append', index = False, schema = 'public')
        print(f"Number of records inserted as reported by the postgres server: {result}.")

    db.disconnect()

    return True


if __name__ == "__main__":
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Descargando archivo de pases a las {current_time}")
    file_path = download()
    if file_path:
        df = parsePases(file_path)
        if df is not None:
            current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"Grabando en la base de datos a las {current_time}")
            graba(df)
            current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"Proceso finalizado a las {current_time}. Borrando archivo descargado.")
            os.remove(file_path)
    else:
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print("Error descargando el archivo. Saliendo a las {current_time}")
        sys.exit(1)