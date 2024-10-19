import os
import tempfile
import pandas as pd
import requests
from urllib3.exceptions import InsecureRequestWarning
import sqlalchemy
from datetime import datetime
from sqlalchemy import create_engine, text

db_user = os.getenv("POSTGRES_USER")
db_password = os.environ.get('POSTGRES_PASSWORD')
db_host = os.environ.get('POSTGRES_HOST')
db_port = os.environ.get('POSTGRES_PORT', '5432')  # Default port for PostgreSQL is 5432
db_name = os.environ.get('POSTGRES_DB')

# Functions block
def download():
    url = "https://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/infomondia.xls"
 

    # Create a temporary directory to store the downloaded file
    temp_dir = tempfile.mkdtemp()

    # File path for the downloaded XLSM file
    file_path = os.path.join(temp_dir, "infomondia.xls")

    # Download the XLS file from the URL
    try:
        requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
        response = requests.get(url, verify=False) # The server's SSL certificate is not verified. This should not be used in production.
        response.raise_for_status()  # Check if the request was successful
        with open(file_path, "wb") as file:
            file.write(response.content)
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print("------------------------------------")
            print(f"File downloaded successfully at {current_time}")
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while downloading the file: {e}")
        return False


    return file_path

def base_monetaria(file_path = None):

    dtypeMap = {
        'date': sqlalchemy.types.Date,
        "cta_cte": sqlalchemy.types.Float,
        "circulacion_monetaria": sqlalchemy.types.Float,
        "base_monetaria_total": sqlalchemy.types.Float
    }
    if file_path == None:

        file_path = download()

    data_df = pd.read_excel(file_path, sheet_name="DATOS | DATA", usecols="D:G", skiprows=25)

    column_definitions = ( 
        "date",
      "cta_cte",
      "circulacion_monetaria",
      "base_monetaria_total")
    
    data_df.columns = column_definitions

    # Filtrar los registros con valores nulos en la columna 'date'
    data_df = data_df.dropna(subset=['date'])
    
    
    data_df.to_sql('infomond_base_monetaria', engine, if_exists='replace', index=False, dtype=dtypeMap)


    if file_path == None:
        os.remove(file_path)

    return True

def tasa_interes(file_path = None):

    dtypeMap = {
        'date': sqlalchemy.types.Date,
        "pase_pasivo": sqlalchemy.types.Float,
        "pase_activo": sqlalchemy.types.Float,
        "tasa": sqlalchemy.types.Float
    }

    if file_path == None:

        file_path = download()

    
    # Read the specified range "A:AF" from the "BASE MONETARIA" sheet, skip first 8 rows
    data_df = pd.read_excel(file_path, sheet_name="DATOS | DATA", usecols="I:L", skiprows=25)


    column_definitions = ( 
        "date",
      "pase_pasivo",
      "pase_activo",
      "tasa")
    
    data_df.columns = column_definitions

    # Filtrar los registros con valores nulos en diversas columnas
    data_df = data_df.dropna(subset=['date'])
    data_df['pase_pasivo'] = data_df['pase_pasivo'].fillna(0)
    data_df['pase_pasivo'] = data_df['pase_pasivo'].astype(str).replace('s/o', 0)
    data_df['pase_pasivo'] = pd.to_numeric(data_df['pase_pasivo'], errors='coerce')

    data_df.to_sql('infomond_tasa_interes', engine, if_exists='replace', index=False, dtype=dtypeMap)

    if file_path == None:
        os.remove(file_path)

    return True


def tipo_cambio(file_path = None):


    dtypeMap = {
        'date': sqlalchemy.types.Date,
        "saldo_reserva_dolar": sqlalchemy.types.Float,
        "tipo_cambio": sqlalchemy.types.Float
    }
    if file_path == None:

        file_path = download()

    data_df = pd.read_excel(file_path, sheet_name="DATOS | DATA", usecols="N:P", skiprows=25)

    column_definitions = ( 
        "date",
      "saldo_reserva_dolar",
      "tipo_cambio")
    
    data_df.columns = column_definitions

    # Filtrar los registros con valores nulos en la columna 'date'
    data_df = data_df.dropna(subset=['date'])
    
    data_df.to_sql('infomond_tipo_cambio', engine, if_exists='replace', index=False, dtype=dtypeMap)


    if file_path == None:
        os.remove(file_path)

    return True

def agreg_monetarios(file_path = None):


    dtypeMap = {
        'date': sqlalchemy.types.Date,
        "base_monetaria": sqlalchemy.types.Float,
        "m2_privado": sqlalchemy.types.Float
    }
    if file_path == None:

        file_path = download()

    data_df = pd.read_excel(file_path, sheet_name="DATOS | DATA", usecols="R:T", skiprows=25)

    column_definitions = ( 
        "date",
      "base_monetaria",
      "m2_privado")
    
    data_df.columns = column_definitions

    # Filtrar los registros con valores nulos en la columna 'date'
    data_df = data_df.dropna(subset=['date'])
    
    data_df.to_sql('infomond_agreg_monetarios', engine, if_exists='replace', index=False, dtype=dtypeMap)


    if file_path == None:
        os.remove(file_path)

    return True

def liquidez_entidades(file_path = None):

    dtypeMap = {
        'date': sqlalchemy.types.Date,
        "liquidez": sqlalchemy.types.Float
    }
    if file_path == None:

        file_path = download()

    data_df = pd.read_excel(file_path, sheet_name="DATOS | DATA", usecols="V:W", skiprows=25)

    column_definitions = ( 
        "date",
      "liquidez")
    
    data_df.columns = column_definitions

    # Filtrar los registros con valores nulos en la columna 'date'
    data_df = data_df.dropna(subset=['date'])
    
    data_df.to_sql('infomond_liquidez_entidades', engine, if_exists='replace', index=False, dtype=dtypeMap)

    if file_path == None:
        os.remove(file_path)

    return True


def tasa_interes_depositos(file_path = None):

    dtypeMap = {
        'date': sqlalchemy.types.Date,
        "badlar": sqlalchemy.types.Float,
        "hasta_100mil": sqlalchemy.types.Float,
        "tm20": sqlalchemy.types.Float
    }
    if file_path == None:

        file_path = download()

    data_df = pd.read_excel(file_path, sheet_name="DATOS | DATA", usecols="Y:AB", skiprows=25)

    column_definitions = ( 
        "date",
      "badlar",
      "hasta_100mil",
      "tm20")
    
    data_df.columns = column_definitions

    # Filtrar los registros con valores nulos en la columna 'date'
    data_df = data_df.dropna(subset=['date'])
    
    data_df.to_sql('infomond_tasa_interes_depositos', engine, if_exists='replace', index=False, dtype=dtypeMap)

    if file_path == None:
        os.remove(file_path)

    return True


def tasa_interes_prestamos(file_path = None):


    dtypeMap = {
        'date': sqlalchemy.types.Date,
        "adelanto_empresas": sqlalchemy.types.Float,
        "personales": sqlalchemy.types.Float
    }
    if file_path == None:

        file_path = download()

    data_df = pd.read_excel(file_path, sheet_name="DATOS | DATA", usecols="AD:AF", skiprows=25)

    column_definitions = ( 
        "date",
      "adelanto_empresas",
      "personales")
    
    data_df.columns = column_definitions

    # Filtrar los registros con valores nulos en la columna 'date'
    data_df = data_df.dropna(subset=['date'])
    
    data_df.to_sql('infomond_tasa_interes_prestamos', engine, if_exists='replace', index=False, dtype=dtypeMap)

    if file_path == None:
        os.remove(file_path)

    return True



def depositos_privados_variaprom30d(file_path = None):

    dtypeMap = {
        'date': sqlalchemy.types.Date,
        "plazo_fijo": sqlalchemy.types.Float,
        "total": sqlalchemy.types.Float,
        "vista": sqlalchemy.types.Float
    }
    if file_path == None:

        file_path = download()

    data_df = pd.read_excel(file_path, sheet_name="DATOS | DATA", usecols="AH:AK", skiprows=25)

    column_definitions = ( 
        "date",
      "plazo_fijo",
      "total",
      "vista")
    
    data_df.columns = column_definitions

    # Filtrar los registros con valores nulos en la columna 'date'
    data_df = data_df.dropna(subset=['date'])
    
    data_df.to_sql('infomond_depositos_privados', engine, if_exists='replace', index=False, dtype=dtypeMap)

    if file_path == None:
        os.remove(file_path)

    return True


def prestamos_privados_variaprom30d(file_path = None):

    dtypeMap = {
        'date': sqlalchemy.types.Date,
        "total": sqlalchemy.types.Float,
        "personales_y_tarjetas": sqlalchemy.types.Float,
        "adelantos_y_documentos": sqlalchemy.types.Float
    }
    if file_path == None:

        file_path = download()

    data_df = pd.read_excel(file_path, sheet_name="DATOS | DATA", usecols="AM:AP", skiprows=25)

    column_definitions = ( 
        "date",
      "total",
      "personales_y_tarjetas",
      "adelantos_y_documentos")
    
    data_df.columns = column_definitions

    # Filtrar los registros con valores nulos en la columna 'date'
    data_df = data_df.dropna(subset=['date'])
    
    data_df.to_sql('infomond_prestamos_privados', engine, if_exists='replace', index=False, dtype=dtypeMap)


    if file_path == None:
        os.remove(file_path)

    return True


def prest_dep_me_privados_variaprom30d(file_path = None):

    dtypeMap = {
        'date': sqlalchemy.types.Date,
        "prestamos": sqlalchemy.types.Float,
        "depositos": sqlalchemy.types.Float
    }
    if file_path == None:

        file_path = download()

    data_df = pd.read_excel(file_path, sheet_name="DATOS | DATA", usecols="AR:AT", skiprows=25)

    column_definitions = ( 
        "date",
      "prestamos",
      "depositos")
    
    data_df.columns = column_definitions

    # Filtrar los registros con valores nulos en la columna 'date'
    data_df = data_df.dropna(subset=['date'])
    
    data_df.to_sql('infomond_prest_dep_otras_monedas_privados', engine, if_exists='replace', index=False, dtype=dtypeMap)

    if file_path == None:
        os.remove(file_path)

    return True



def pasivos_pesos_bcra(file_path = None):

    dtypeMap = {
        'date': sqlalchemy.types.Date,
        "pasivo": sqlalchemy.types.Float
    }
    if file_path == None:

        file_path = download()

    data_df = pd.read_excel(file_path, sheet_name="DATOS | DATA", usecols="AV:AW", skiprows=25)

    column_definitions = ( 
        "date",
      "pasivo")
    
    data_df.columns = column_definitions

    # Filtrar los registros con valores nulos en la columna 'date'
    data_df = data_df.dropna(subset=['date'])
    
    data_df.to_sql('infomond_pasivos_pesos_bcra', engine, if_exists='replace', index=False, dtype=dtypeMap)

    if file_path == None:
        os.remove(file_path)

    return True


def factores_variacion(file_path = None):

    dtypeMap = {
        'date': sqlalchemy.types.Date,
        "compra_divisas": sqlalchemy.types.Float,
        "tesoro_nacional": sqlalchemy.types.Float,
        "pases": sqlalchemy.types.Float,
        "operaciones_lefi": sqlalchemy.types.Float,
        "otros": sqlalchemy.types.Float,
        "base_monetaria": sqlalchemy.types.Float
    }
    if file_path == None:

        file_path = download()

    data_df = pd.read_excel(file_path, sheet_name="DATOS | DATA", usecols="AY:BE", skiprows=25)

    column_definitions = ( 
        "date",
      "compra_divisas",
      "tesoro_nacional",
      "pases",
      "operaciones_lefi",
      "otros",
      "base_monetaria")
    
    data_df.columns = column_definitions

    # Filtrar los registros con valores nulos en la columna 'date'
    data_df = data_df.dropna(subset=['date'])
    
    data_df.to_sql('infomond_factores_variacion', engine, if_exists='replace', index=False, dtype=dtypeMap)

    if file_path == None:
        os.remove(file_path)

    return True


if __name__ == "__main__":
    file_path = download()

    engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
  
    for func in [base_monetaria,tasa_interes,tipo_cambio,agreg_monetarios,liquidez_entidades,
            tasa_interes_depositos,tasa_interes_prestamos,depositos_privados_variaprom30d,prestamos_privados_variaprom30d,
            prest_dep_me_privados_variaprom30d,pasivos_pesos_bcra,factores_variacion]:
        if func(file_path):
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"{func.__name__} parsed successfully at {current_time}")
        else:
            print(f"An error occurred while downloading {func.__name__}")
    os.remove(file_path)
    print("Fin del proceso a las: ",datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
