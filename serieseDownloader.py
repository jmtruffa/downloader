import os
import tempfile
import pandas as pd
import requests
from dataBaseConn import DatabaseConnection
from janitor import cleanNames

def downloadBaseMonetaria():
    url = "https://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/series.xlsm"

    # Create a temporary directory to store the downloaded file
    temp_dir = tempfile.mkdtemp()

    # File path for the downloaded XLSM file
    file_path = os.path.join(temp_dir, "data.xlsm")

    # Download the XLSM file from the URL
    response = requests.get(url)
    with open(file_path, "wb") as file:
        file.write(response.content)

    # Read the specified range "A:AF" from the "BASE MONETARIA" sheet, skip first 8 rows
    data_df = pd.read_excel(file_path, sheet_name="BASE MONETARIA", usecols="A:AF", skiprows=8)

    #data_df["date"] = data_df["Fecha"]
    
    # Drop columns B, P, and X
    columns_to_drop = [1, 15, 23]
    data_df = data_df.drop(columns=data_df.columns[columns_to_drop])

    column_definitions = ("date",
      "vdFeTotal",
      "vdFeComSP",
      "vdFeComTN",
      "vdFeOtrasOpTNAT",
      "vdFeOtrasOpTNTU",
      "vdFeOtrasOpTNResto",
      "vdFePasesLeliqPases",
      "vdFePasesLeliqLeliqNotaliq",
      "vdFePasesLeliqRedescuentos",
      "vdFePasesLeliqIntereses",
      "vdFeLebacNobac",
      "vdFeRescateCuasi",
      "vdFeOtros",
      "vdBMCMBilletesPublico",
      "vdBMCMBilletesEntidades",
      "vdBMCMBilletesChequesCan",
      "vdBMCtaCteEnBCRA",
      "vdBMTotalsinCuasi",
      "vdBMMasCuasiCuasimonedas",
      "vdVBMTotal",
      "sdBMCMBilletesPublico",
      "sdBMCMBilletesEntidades",
      "sdBMCMBilletesChequesCan",
      "sdBMCtaCteEnBCRA",
      "sdBMTotalsinCuasi",
      "sdBMMasCuasiCuasimonedas",
      "sdVBMTotal",
      "tipoSerie")
    
    data_df.columns = column_definitions
    # Rename columns
    #data_df.columns = ["date"] + [f"col_{i}" for i in range(1, len(data_df.columns))]

    # Convert the "date" column to Unix timestamp
    data_df["date"] = pd.to_datetime(data_df["date"]).view('int64') // 10**9

    # Initialize the database connection abstraction
    db = DatabaseConnection("/Users/juan/data/dataBCRA.sqlite3")
    db.connect()

    # Check if the table exists
    if not db.execute_select_query("SELECT name FROM sqlite_master WHERE type='table' AND name='bmBCRA'"):
        # Define column names and types
        #column_definitions = ", ".join([f"{col} INTEGER" if col == "date" else f"{col} REAL" for col in data_df.columns])
        #column_definitions = cleanNames(data_df).dtypes.to_dict()
        

        columnDefinitionsSQL = ", ".join([f"{col} INTEGER" if col == "date" else f"{col} REAL" for col in column_definitions])
        

        db.create_table("bmBCRA", columnDefinitionsSQL)

    # # Query the last date in the existing table
    # last_date_query = "SELECT MAX(date) FROM bmBCRA"
    # last_date_result = db.execute_select_query(last_date_query)
    # last_date = last_date_result[0][0] if last_date_result[0][0] else 0

    # new_rows = data_df[data_df["date"] > last_date]

    #Insert new rows into the table using executemany
    # Dump data into table
    #new_rows_data = [{"date": int(row["date"]), "col_1": row["col_1"], "col_2": row["col_2"], ...} for _, row in data_df.iterrows()]
    
    data_to_insert = data_df.to_dict(orient="records")

    db.insert_data_many("bmBCRA", data_to_insert, overwrite=True)
    
    db.disconnect()

    # Delete the temporary file
    os.remove(file_path)

    return True

# Example usage
if __name__ == "__main__":
    downloadBaseMonetaria()
    print("Data updated in the database.")
