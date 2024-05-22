import os
import tempfile
import pandas as pd
import requests
from urllib3.exceptions import InsecureRequestWarning
from dataBaseConn2 import DatabaseConnection
from datetime import datetime
import sqlalchemy

currentTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def downloadCER():
    url = "http://www.bcra.gov.ar/Pdfs/PublicacionesEstadisticas/diar_cer.xls"
    # Create a temporary directory to store the downloaded file
    temp_dir = tempfile.mkdtemp()

    # File path for the downloaded XLS file
    file_path = os.path.join(temp_dir, "data.xls")

    # Download the XLS file from the URL 
    try:
        # Disable SSL certificate verification and warnings to avoid errors
        requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
        response = requests.get(url, verify=False)           
        response.raise_for_status()  # Check if the request was successful
        with open(file_path, "wb") as file:
            file.write(response.content)
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while downloading the file: {e}")
        return False
    
    print("------------------------------------")
    print(f"CER downloaded successfully at {currentTime}")

    # Read the specified range "A:B" from the first sheet
    data_df = pd.read_excel(file_path, sheet_name=0, usecols="A:B", skiprows=26)
    
    # Set column names
    data_df.columns = ["date", "CER"]

    # Convert the "date" column to datetime
    data_df["date"] = pd.to_datetime(data_df["date"], format="%d/%m/%Y") 

    # connect to the database
    print(f"Connecting to the database...{os.environ.get('POSTGRES_DB')} on server {os.environ.get('POSTGRES_HOST')}, port {os.environ.get('POSTGRES_PORT')}")
    db = DatabaseConnection(db_type="postgresql", db_name= os.environ.get('POSTGRES_DB'))
    db.connect()

    # Check if the table exists
    table_exists_query = f"""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public' AND table_name = 'CER'
    """
    result = pd.read_sql(table_exists_query, db.conn)

    if result.empty:
        data_to_insert = data_df
    else:
        query = f'SELECT MAX(date) FROM "CER"'
        last_date = pd.read_sql(query, db.conn).iloc[0,0]
        # convert to datetime in order to compare to the date column in data_df
        last_date = pd.to_datetime(last_date)
        
        data_to_insert = data_df[data_df['date'] > last_date]
    
    if len(data_to_insert) == 0:
        print("No rows to be inserted. Exiting...")
    else:
        print(f"Inserting {len(data_to_insert)} rows into CER Table")
        # use Date type for the 'date' column in the database to get rid of the time part
        dtypeMap = {'date': sqlalchemy.types.Date}
        result = data_to_insert.to_sql(name = 'CER', con = db.engine, if_exists = 'append', index = False, dtype=dtypeMap, schema = 'public')
        #db.conn.commit()
        print(f"Number of records inserted as reported by the postgres server: {result}") 
    
    db.disconnect()

    # Delete the temporary file
    os.remove(file_path)    

    return True

# Example usage
if __name__ == "__main__":

    if downloadCER() == False:
        print("An error occurred while downloading CER")

