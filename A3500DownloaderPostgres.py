import os
import tempfile
import pandas as pd
import requests
from dataBaseConn2 import DatabaseConnection
from datetime import datetime
import sqlalchemy

currentTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def downloadA3500():
    url = "https://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/com3500.xls"
    # Create a temporary directory to store the downloaded file
    temp_dir = tempfile.mkdtemp()

    # File path for the downloaded XLS file
    file_path = os.path.join(temp_dir, "data.xls")

    # Download the XLS file from the URL
    try:
        response = requests.get(url)
        response.raise_for_status()  # Check if the request was successful
        with open(file_path, "wb") as file:
            file.write(response.content)
    except requests.exceptions.RequestException as e:    
        print(f"An error occurred while downloading the file: {e} a las {currentTime}")
        return False
    
    print("------------------------------------")
    print("A3500 downloaded successfully at " + currentTime)
    
    # Read the specified range "A:B" from the first sheet
    data_df = pd.read_excel(file_path, sheet_name=0, usecols="C:D", skiprows=3)

    # Set column names
    data_df.columns = ["date", "A3500"]

    # Convert the "date" column to datetime
    date_format = "%d/%m/%Y"
    data_df['date'] = pd.to_datetime(data_df['date'], format=date_format)

    # connect to the database
    db = DatabaseConnection(db_type="postgresql", db_name= "data")
    db.connect()

    # Check if the table exists
    table_exists_query = f"""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public' AND table_name = 'A3500'
    """
    result = pd.read_sql(table_exists_query, db.conn)

    if result.empty:
        data_to_insert = data_df
    else:
        query = f'SELECT MAX(date) FROM "A3500"'
        last_date = pd.read_sql(query, db.conn).iloc[0,0]
        # convert to datetime in order to compare to the date column in data_df
        last_date = pd.to_datetime(last_date)
        
        data_to_insert = data_df[data_df['date'] > last_date]
    
    if len(data_to_insert) == 0:
        print("No rows to be inserted. Exiting...")
    else:
        print(f"Inserting {len(data_to_insert)} rows into A3500 Table")
        # use Date type for the 'date' column in the database to get rid of the time part
        dtypeMap = {'date': sqlalchemy.types.Date}
        result = data_to_insert.to_sql(name = 'A3500', con = db.engine, if_exists = 'append', index = False, dtype=dtypeMap, schema = 'public')
        print(f"Number of records inserted as reported by the postgres server: {result}") 
    

    db.disconnect()

    # Delete the temporary file
    os.remove(file_path)

    return True

if __name__ == "__main__":
    if downloadA3500() == False:
        print("An error occurred while downloading A3500")
        
