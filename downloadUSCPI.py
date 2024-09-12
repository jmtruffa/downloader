from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import os
import pandas as pd
from io import StringIO
import time
from dataBaseConn2 import DatabaseConnection
import sqlalchemy

db_user = os.environ.get('POSTGRES_USER')
db_password = os.environ.get('POSTGRES_PASSWORD')
db_host = os.environ.get('POSTGRES_HOST')
db_port = os.environ.get('POSTGRES_PORT', '5432')  # Default port for PostgreSQL is 5432
db_name = os.environ.get('POSTGRES_DB')
dtypeMap = {'date': sqlalchemy.types.Date}

def download_USCPI_data():
    """Descarga los datos de USCPI desde la web utilizando Selenium y retorna un DataFrame filtrado por series_id"""
    
    url = "https://download.bls.gov/pub/time.series/cu/cu.data.1.AllItems"

   # Debe ser corrido con xvfb-run -a downloadUSCPI para que funcione 
    
   #  Configurar Selenium para usar Chrome en modo headless
    chrome_options = Options()
   # chrome_options.add_argument('--headless')

    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")

    driver = webdriver.Chrome(options=chrome_options)
    

    try:
        # Navegar a la URL
        driver.get(url)
        
        # Esperar unos segundos para asegurarse de que la página se cargue completamente
        time.sleep(5)

        # Obtener el contenido de la página
        page_source = driver.page_source
        
        # Cerrar el driver después de obtener el contenido
        driver.quit()

        # Convertir el contenido descargado a un DataFrame
        data = StringIO(page_source)
        df = pd.read_csv(data, sep='\t', engine='python')

        colnames = ['series_id', 'year', 'period', 'value', 'footnote_codes']
        df.columns = colnames
        
        # Limpiar las columnas para quitar espacios en los encabezados
        #df.columns = df.columns.str.strip()

        # Filtrar por series_id = "CUUR0000SA0"
        df['series_id'] = df['series_id'].str.strip()  # Remove leading and trailing spaces from 'series_id'
        df_filtered = df[df['series_id'] == "CUUR0000SA0"]
        
        return df_filtered

    except Exception as e:
        print(f"Error durante la descarga con Selenium: {e}")
        driver.quit()
        return None

def save_to_postgres(df):
    """Sobrescribe la tabla USCPI en la base de datos con los datos descargados"""
    
    # Conectar a la base de datos
    engine = sqlalchemy.create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
    
    # Sobrescribir la tabla 'USCPI'
    if not df.empty:
        print(f"Guardando {len(df)} filas en la tabla 'USCPI'.")
        dtype_map = {'year': sqlalchemy.types.Integer, 'value': sqlalchemy.types.Float}
        
        # Guardar el DataFrame en PostgreSQL, sobrescribiendo la tabla existente
        df.to_sql(name='USCPI', con=engine, if_exists='replace', index=False, dtype=dtype_map, schema='public')
        
        print("Datos guardados correctamente en la tabla USCPI.")
    else:
        print("No hay datos para guardar.")
    

if __name__ == "__main__":
    # Descargar los datos filtrados usando Selenium
    df_USCPI = download_USCPI_data()

    if df_USCPI is not None:
        # Guardar los datos en PostgreSQL
        save_to_postgres(df_USCPI)
    else:
        print("El proceso no obtuvo datos válidos. Abortando.")