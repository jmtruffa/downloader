import os
import tempfile
import pandas as pd
import requests
from dataBaseConn2 import DatabaseConnection
from datetime import datetime
import sqlalchemy

currentTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def downloadITCRM():
    url = "https://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/ITCRMSerie.xlsx"
    # Create a temporary directory to store the downloaded file
    temp_dir = tempfile.mkdtemp()

    # File path for the downloaded XLS file
    file_path = os.path.join(temp_dir, "data.xlsx")
    

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
    print("ITCRM downloaded successfully at " + currentTime)

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



    # Convert the "date" column to datetime format, ignoring text
    data_df["date"] = pd.to_datetime(data_df["date"], errors='coerce')
    # Filter out rows with NaT (text)
    data_df = data_df.dropna(subset=["date"])

    # Convert all the column but "date" to numeric
    data_df[["ITCRM", "ITCRBBrasil", "ITCRBCanada", "ITCRBChile", "ITCRBEEUU", "ITCRBMexico", "ITCRMUruguay", "ITCRMChina", "ITCRMIndia", "ITCRMJapon", "ITCRMUK",
                "ITCRMSuiza", "ITCRMZonaEuro", "ITCRMVietname", "ITCRMSudamerica"]] = data_df[["ITCRM", "ITCRBBrasil", "ITCRBCanada", "ITCRBChile", "ITCRBEEUU",
                                                                                                "ITCRBMexico", "ITCRMUruguay", "ITCRMChina", "ITCRMIndia", "ITCRMJapon", "ITCRMUK",
                                                                                                "ITCRMSuiza", "ITCRMZonaEuro", "ITCRMVietname", "ITCRMSudamerica"]].apply(pd.to_numeric, errors='coerce')
    
    
    db = DatabaseConnection(db_type="postgresql", db_name= "data")
    db.connect()
    
    # Not need to check if table exists, since ITCRM is constantly recalculated backwards

    if len(data_df) == 0:
        print("No rows to be inserted. Exiting...")
    else:
        print(f"Inserting {len(data_df)} rows into ITCRM Table")
        # use Date type for the 'date' column in the database to get rid of the time part
        dtypeMap = {'date': sqlalchemy.types.Date}
        result = data_df.to_sql(name = 'ITCRM', con = db.engine, if_exists = 'replace', index = False, dtype=dtypeMap, schema = 'public')
        db.conn.commit()
        print(f"Number of records inserted as reported by the postgres server: {result}") 
    
    # Disconnect from the database
    db.disconnect()

    print("ITCRM updated successfully at " + currentTime)
    

    # Delete the temporary file
    print("Removing temporary files...")
    os.remove(file_path)

    return True

# Example usage
if __name__ == "__main__":
    #currentTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if downloadITCRM()== False:
        print(f"An error occurred while downloading ITCRM at {currentTime}")


    