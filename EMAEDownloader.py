import os
import tempfile
import pandas as pd
import requests
from dataBaseConn import DatabaseConnection

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
    data_df["date"] = pd.to_datetime(data_df["date"], dayfirst=True).view('int64') // 10**9
    
    # Initialize the database connection abstraction
    db = DatabaseConnection("/Users/juan/data/economicData.sqlite3")
    db.connect()

    # Check if the table exists
    if not db.execute_select_query("SELECT name FROM sqlite_master WHERE type='table' AND name='EMAE'"):
        db.create_table("EMAE", "date INTEGER, EMAE REAL, EMAEVarAnual REAL, EMAEDesest REAL, EMAEDesestVarMensual REAL, EMAETendenciaCiclo REAL, EMAETendenciaCiclo_var_mensual REAL")

    # Query the last date in the existing table
    last_date_query = "SELECT MAX(date) FROM EMAE"
    last_date_result = db.execute_select_query(last_date_query)
    last_date = last_date_result[0][0] if last_date_result[0][0] else 0

    new_rows = data_df[data_df["date"] > last_date]

    if not new_rows.empty:
    # Prepare data for insertion
        new_rows_data = [
            {
                "date": int(row["date"]),
                "EMAE": row["EMAE"],
                "EMAEVarAnual": row["EMAEVarAnual"],
                "EMAEDesest": row["EMAEDesest"],
                "EMAEDesestVarMensual": row["EMAEDesestVarMensual"],
                "EMAETendenciaCiclo": row["EMAETendenciaCiclo"],
                "EMAETendenciaCiclo_var_mensual": row["EMAETendenciaCiclo_var_mensual"]
            }
            for _, row in new_rows.iterrows()
        ]
    
        # Insert new rows into the table using executemany
        db.insert_data_many("EMAE", new_rows_data)
    
    db.disconnect()

    # Delete the temporary file
    os.remove(file_path)


    return True

# Example usage
if __name__ == "__main__":
    downloadEMAE()
    if downloadEMAE():
        print("Data updated in the database.")
    else:
        print("Data download failed.")