import os
import tempfile
import pandas as pd
import requests
from dataBaseConn import DatabaseConnection

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
        print(f"An error occurred while downloading the file: {e}")
        return False

    # Read the specified range "A:B" from the first sheet
    data_df = pd.read_excel(file_path, sheet_name=0, usecols="C:D", skiprows=3)

    # Set column names
    data_df.columns = ["date", "A3500"]
 
    # Convert the "date" column to numeric (Unix timestamps)
    data_df["date"] = pd.to_datetime(data_df["date"], dayfirst=True).view('int64') // 10**9
    
    # Initialize the database connection abstraction
    db = DatabaseConnection("/Users/juan/data/economicData.sqlite3")
    db.connect()

    # Check if the table exists
    if not db.execute_select_query("SELECT name FROM sqlite_master WHERE type='table' AND name='A3500'"):
        db.create_table("A3500", "date INTEGER, A3500 REAL")

    # Query the last date in the existing table
    last_date_query = "SELECT MAX(date) FROM A3500"
    last_date_result = db.execute_select_query(last_date_query)
    last_date = last_date_result[0][0] if last_date_result[0][0] else 0

    new_rows = data_df[data_df["date"] > last_date]

    if not new_rows.empty:
        # Insert new rows into the table using executemany
        new_rows_data = [{"date": int(row["date"]), "A3500": row["A3500"]} for _, row in new_rows.iterrows()]
        db.insert_data_many("A3500", new_rows_data)
    
    db.disconnect()

    # Delete the temporary file
    os.remove(file_path)

    return True

if __name__ == "__main__":
    downloadA3500()
    if downloadA3500():
        print("A3500 downloaded successfully")
    else:
        print("An error occurred while downloading A3500")
        
