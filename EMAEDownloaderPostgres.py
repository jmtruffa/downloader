import os
import tempfile
import pandas as pd
import requests
from dataBaseConn2 import DatabaseConnection
import sqlalchemy
import datetime

def downloadEMAE():
    url = "https://www.indec.gob.ar/ftp/cuadros/economia/sh_emae_mensual_base2004.xls"
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

    # Read the specified range "A:B" from the first sheet
    data_df = pd.read_excel(file_path, sheet_name=0, usecols="C:H", skiprows=4, header=None)

    
    
    # Set column names
    data_df.columns = ["EMAE", "EMAEVarAnual", "EMAEDesest", "EMAEDesestVarMensual", "EMAETendenciaCiclo", "EMAETendenciaCiclo_var_mensual"]

    date_range = pd.date_range("2004-01-01", periods=len(data_df), freq="MS")

    # Add the date column to the beginning of the DataFrame
    data_df.insert(0, "date", date_range)

    # Get rid of NaN rows
    data_df = data_df.dropna(subset=["EMAE"])

    # Convert the "date" column to numeric (Unix timestamps)
    #data_df["date"] = pd.to_datetime(data_df["date"], 
    
    # connect to the database
    db = DatabaseConnection(db_type="postgresql", db_name=os.environ.get('POSTGRES_DB'))
    db.connect()

    # Not need to check if table exists, since EMAE is constantly recalculated backwards

    # Check if there are rows to be inserted
    if len(data_df) == 0:
        print("No rows to be inserted. Exiting...")
    else:
        time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print("-" * 80)
        print(f"Inserting {len(data_df)} rows into EMAE Table at {time}")
        # use Date type for the 'date' column in the database to get rid of the time part
        dtypeMap = {'date': sqlalchemy.types.Date}
        result = data_df.to_sql(name = 'EMAE', con = db.engine, if_exists = 'replace', index = False, dtype=dtypeMap, schema = 'public')
        #db.conn.commit()
        print(f"Number of records inserted as reported by the postgres server: {result}") 

    
    db.disconnect()

    # Delete the temporary file
    os.remove(file_path)


    return True

# Example usage
if __name__ == "__main__":
    if downloadEMAE():
        print("Data updated in the database.")
    else:
        print("Data download failed.")