import os
import tempfile
import pandas as pd
import requests
from dataBaseConn import DatabaseConnection

def downloadITCRM():
    url = "https://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/ITCRMSerie.xlsx"
    # Create a temporary directory to store the downloaded file
    temp_dir = tempfile.mkdtemp()

    # File path for the downloaded XLS file
    file_path = os.path.join(temp_dir, "data.xlsx")

    # Download the XLS file from the URL
    response = requests.get(url)
    with open(file_path, "wb") as file:
        file.write(response.content)

    # Read the Excel file until an empty row is encountered
    data_rows = []
    with pd.ExcelFile(file_path) as xls:
        sheet_name = xls.sheet_names[0]
        for row in pd.read_excel(xls, sheet_name, header=None, skiprows=2, dtype=str).values:
            if pd.notna(row[0]):
                data_rows.append(row)
            else:
                break
    # Convert the data rows to a DataFrame
    data_df = pd.DataFrame(data_rows, columns=["date", "ITCRM", "ITCRBBrasil", "ITCRBCanada", "ITCRBChile", "ITCRBEEUU",
                                               "ITCRBMexico", "ITCRMUruguay", "ITCRMChina", "ITCRMIndia", "ITCRMJapon", "ITCRMUK",
                                               "ITCRMSuiza", "ITCRMZonaEuro", "ITCRMVietname", "ITCRMSudamerica"])


    # Convert the "date" column to numeric (Unix timestamps)
    data_df["date"] = pd.to_datetime(data_df["date"]).view('int64') // 10**9

    # Initialize the database connection abstraction
    db = DatabaseConnection("/Users/juan/data/economicData.sqlite3")
    db.connect()

    # Check if the table exists and create if needed
    if not db.execute_select_query("SELECT name FROM sqlite_master WHERE type='table' AND name='ITCRM'"):
        db.create_table("ITCRM", "date INTEGER, ITCRM REAL, ITCRBBrasil REAL, ITCRBCanada REAL, ITCRBChile REAL, "
                                 "ITCRBEEUU REAL, ITCRBMexico REAL, ITCRMUruguay REAL, ITCRMChina REAL, "
                                 "ITCRMIndia REAL, ITCRMJapon REAL, ITCRMUK REAL, ITCRMSuiza REAL, "
                                 "ITCRMZonaEuro REAL, ITCRMVietname REAL, ITCRMSudamerica REAL")

    # Query the last date in the existing table
    last_date_query = "SELECT MAX(date) FROM ITCRM"
    last_date_result = db.execute_select_query(last_date_query)
    last_date = last_date_result[0][0] if last_date_result[0][0] else 0

    # Filter new rows based on the last date in the table
    new_rows = data_df[data_df["date"] > last_date]

    if not new_rows.empty:
        # Insert new rows into the table using executemany
        new_rows_data = [{"date": int(row["date"]), "ITCRM": row["ITCRM"], "ITCRBBrasil": row["ITCRBBrasil"],
                        "ITCRBCanada": row["ITCRBCanada"], "ITCRBChile": row["ITCRBChile"], "ITCRBEEUU": row["ITCRBEEUU"],
                        "ITCRBMexico": row["ITCRBMexico"], "ITCRMUruguay": row["ITCRMUruguay"], "ITCRMChina": row["ITCRMChina"],
                        "ITCRMIndia": row["ITCRMIndia"], "ITCRMJapon": row["ITCRMJapon"], "ITCRMUK": row["ITCRMUK"],
                        "ITCRMSuiza": row["ITCRMSuiza"], "ITCRMZonaEuro": row["ITCRMZonaEuro"], "ITCRMVietname": row["ITCRMVietname"],
                        "ITCRMSudamerica": row["ITCRMSudamerica"]} for _, row in new_rows.iterrows()]
        
        db.insert_data_many("ITCRM", new_rows_data)

    # Disconnect from the database
    db.disconnect()

    # Delete the temporary file
    os.remove(file_path)

    return True

# Example usage
if __name__ == "__main__":
    downloadITCRM()
    print("Data updated in the database.")


    