import os
import tempfile
import pandas as pd
import requests
from dataBaseConn2 import DatabaseConnection
import sqlalchemy
from datetime import datetime

currentTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def downloadUVA():
    url = "http://www.bcra.gov.ar/Pdfs/PublicacionesEstadisticas/diar_uva.xls"
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
        print(f"An error occurred while downloading the file: {e}")
        return False
    
    print("------------------------------------")
    print(f"UVA downloaded successfully at {currentTime}")

    # # Download the XLS file from the URL
    # response = requests.get(url)
    # with open(file_path, "wb") as file:
    #     file.write(response.content)

    # Read the specified range "A:B" from the first sheet
    data_df = pd.read_excel(file_path, sheet_name=0, usecols="A:B", skiprows=27, header= None)

    # Set column names
    data_df.columns = ["date", "UVA"]
 
    data_df['date'] = pd.to_datetime(data_df['date'], format="%d/%m/%Y")

    # # Convert the "date" column to numeric (Unix timestamps)
    # data_df["date"] = pd.to_datetime(data_df["date"], dayfirst=True).view('int64') // 10**9
    
    # Initialize the database connection abstraction
    db = DatabaseConnection(db_type="postgresql", db_name= "data")
    db.connect()

    # Check if the table exists
    table_exists_query = f"""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public' AND table_name = 'UVA'
    """
    result = pd.read_sql(table_exists_query, db.conn)

    if result.empty:
        data_to_insert = data_df
    else:
        query = f'SELECT MAX(date) FROM "UVA"'
        last_date = pd.read_sql(query, db.conn).iloc[0,0]
        # convert to datetime in order to compare to the date column in data_df
        last_date = pd.to_datetime(last_date)
        
        data_to_insert = data_df[data_df['date'] > last_date]

    if len(data_to_insert) == 0:
        print("No rows to be inserted. Exiting...")
    else:
        print(f"Inserting {len(data_to_insert)} rows into UVA Table")
        # use Date type for the 'date' column in the database to get rid of the time part
        dtypeMap = {'date': sqlalchemy.types.Date}
        result = data_to_insert.to_sql(name = 'UVA', con = db.engine, if_exists = 'append', index = False, dtype=dtypeMap, schema = 'public')
        db.conn.commit()
        print(f"Number of records inserted as reported by the postgres server: {result}") 

    # Close the database connection
    db.disconnect()

    # Delete the temporary file
    os.remove(file_path)

    return True

# Example usage
if __name__ == "__main__":
    downloadUVA()
