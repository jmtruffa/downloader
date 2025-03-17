import os
import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text

# Load environment variables for PostgreSQL connection
db_user = os.environ.get('POSTGRES_USER')
db_password = os.environ.get('POSTGRES_PASSWORD')
db_host = os.environ.get('POSTGRES_HOST')
db_port = os.environ.get('POSTGRES_PORT', '5432')
db_name = os.environ.get('POSTGRES_DB')

DATABASE_URL = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
engine = create_engine(DATABASE_URL)

# API URL
API_URL = "https://www.mae.com.ar/mercados/forex/api/LeerForexPrecios"

# Regex pattern to extract currency details from 'Titulo'
pattern = r'(?P<currency_out>[A-Z]+)\s*/\s*(?P<currency_in>[A-Z]+)\s*(?P<settle>\d+)\s*(?P<settle_date>\d{6})'

def fetch_forex_data():
    """Fetch forex data from API and return a processed DataFrame."""
    response = requests.get(API_URL)
    if response.status_code != 200:
        print("Failed to fetch data from API.")
        return None
    
    data = response.json().get("data", [])
    if not data:
        print("No data received from API.")
        return None
    
    df = pd.DataFrame(data)
    
    # Convert 'Fecha' to proper date format
    df['date'] = pd.to_datetime(df['Fecha'], format='%y%m%d', errors='coerce')
    
    # Extract currency details from 'Titulo'
    df_extracted = df['Titulo'].str.extract(pattern)
    
    # Convert 'Monto' to float
    df['monto'] = df['Monto'].str.replace(',', '').astype(float)
    
    # Convert 'Cotizacion' to float
    df['cotizacion'] = df['Cotizacion'].astype(float)
    
    # Convert 'Hora' to time format
    df['hora'] = pd.to_datetime(df['Hora'], format='%H:%M:%S').dt.time
    
    # Rename columns to match database structure
    df = pd.concat([df, df_extracted], axis=1)
    df.rename(columns={'Titulo': 'instrumento', 'Rueda': 'rueda'}, inplace=True)
    
    return df[['date', 'rueda', 'instrumento', 'currency_out', 'currency_in', 'settle', 'settle_date', 'monto', 'cotizacion', 'hora']]

def save_to_database(df):
    """Save the forex data to the PostgreSQL database."""
    if df is None or df.empty:
        print("No data to save.")
        return
    
    with engine.connect() as conn:
        # Check last inserted date
        last_date_query = text('SELECT MAX(date) FROM public.forex2')
        result = conn.execute(last_date_query).scalar()
        
        if result:
            last_date = pd.to_datetime(result)
            df = df[df['date'] > last_date]
        
        if df.empty:
            print("No new data to insert.")
        else:
            df.to_sql('forex2', con=engine, if_exists='append', index=False, schema='public')
            print(f"Inserted {len(df)} rows into forex2 table.")

if __name__ == "__main__":
    print("---------------------------------------------")
    currentTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Iniciando maeScraper a las: {currentTime}")
    forex_data = fetch_forex_data()
    if forex_data is not None:
        save_to_database(forex_data)
    else:
        print("Data fetching failed.")
    currentTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Proceso finalizado a las: {currentTime}")
    print("---------------------------------------------")