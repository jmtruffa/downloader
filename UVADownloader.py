import os
import tempfile
import pandas as pd
import requests
from dataBaseConn import DatabaseConnection

def downloadUVA():
    url = "http://www.bcra.gov.ar/Pdfs/PublicacionesEstadisticas/diar_uva.xls"
    # Create a temporary directory to store the downloaded file
    temp_dir = tempfile.mkdtemp()

    # File path for the downloaded XLS file
    file_path = os.path.join(temp_dir, "data.xls")

    # Download the XLS file from the URL
    response = requests.get(url)
    with open(file_path, "wb") as file:
        file.write(response.content)

    # Read the specified range "A:B" from the first sheet
    data_df = pd.read_excel(file_path, sheet_name=0, usecols="A:B", skiprows=27, header= None)

    # Set column names
    data_df.columns = ["date", "UVA"]
 
    # Convert the "date" column to numeric (Unix timestamps)
    data_df["date"] = pd.to_datetime(data_df["date"], dayfirst=True).view('int64') // 10**9
    
    # Initialize the database connection abstraction
    db = DatabaseConnection("/home/juant/data/economicData.sqlite3")
    db.connect()

    # Check if the table exists
    if not db.execute_select_query("SELECT name FROM sqlite_master WHERE type='table' AND name='UVA'"):
        db.create_table("UVA", "date INTEGER, UVA REAL")

    # Query the last date in the existing table
    last_date_query = "SELECT MAX(date) FROM UVA"
    last_date_result = db.execute_select_query(last_date_query)
    last_date = last_date_result[0][0] if last_date_result[0][0] else 0

    new_rows = data_df[data_df["date"] > last_date]

    if not new_rows.empty:
        # Insert new rows into the table using executemany
        new_rows_data = [{"date": int(row["date"]), "UVA": row["UVA"]} for _, row in new_rows.iterrows()]
        db.insert_data_many("UVA", new_rows_data)
    
    db.disconnect()

    # Delete the temporary file
    os.remove(file_path)

    return True

# Example usage
if __name__ == "__main__":
    downloadUVA()
    print("Data updated in the database.")
