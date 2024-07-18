import os
import tempfile
import pandas as pd
import requests
from urllib3.exceptions import InsecureRequestWarning
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
        requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
        response = requests.get(url, verify=False) 
        response.raise_for_status()  # Check if the request was successful
        with open(file_path, "wb") as file:
            file.write(response.content)
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while downloading the file: {e} a las {currentTime}")
        return False
    
    print("------------------------------------")
    print("ITCRM downloaded successfully at " + currentTime)

    print(f"Reading the Excel file...{file_path}")
    data_df = pd.read_excel(file_path, skiprows=1)
    print("Excel file read successfully")
    # Rename the columns
    columns = columns=["date", "ITCRM", "ITCRBBrasil", "ITCRBCanada", "ITCRBChile", "ITCRBEEUU",
                                               "ITCRBMexico", "ITCRMUruguay", "ITCRMChina", "ITCRMIndia", "ITCRMJapon", "ITCRMUK",
                                               "ITCRMSuiza", "ITCRMZonaEuro", "ITCRMVietname", "ITCRMSudamerica"]
    data_df.columns = columns

    # Convert the "date" column to datetime format, ignoring text with errors='coerce'
    data_df["date"] = pd.to_datetime(data_df["date"], errors='coerce')
    # Filter out rows with NaT (text)
    data_df = data_df.dropna(subset=["date"])

    # Convert all the column but "date" to numeric
    data_df[["ITCRM", "ITCRBBrasil", "ITCRBCanada", "ITCRBChile", "ITCRBEEUU", "ITCRBMexico", "ITCRMUruguay", "ITCRMChina", "ITCRMIndia", "ITCRMJapon", "ITCRMUK",
                "ITCRMSuiza", "ITCRMZonaEuro", "ITCRMVietname", "ITCRMSudamerica"]] = data_df[["ITCRM", "ITCRBBrasil", "ITCRBCanada", "ITCRBChile", "ITCRBEEUU",
                                                                                                "ITCRBMexico", "ITCRMUruguay", "ITCRMChina", "ITCRMIndia", "ITCRMJapon", "ITCRMUK",
                                                                                                "ITCRMSuiza", "ITCRMZonaEuro", "ITCRMVietname", "ITCRMSudamerica"]].apply(pd.to_numeric, errors='coerce')
    
    
    
    
    # Not need to check if table exists, since ITCRM is constantly recalculated backwards

    if len(data_df) == 0:
        print("No rows to be inserted. Exiting...")
        return False
    
    db = DatabaseConnection(db_type="postgresql", db_name=os.environ.get('POSTGRES_DB'))
    db.connect()

    print(f"Inserting {len(data_df)} rows into ITCRM Table")
    # use Date type for the 'date' column in the database to get rid of the time part
    dtypeMap = {'date': sqlalchemy.types.Date}
    result = data_df.to_sql(name = 'ITCRM', con = db.engine, if_exists = 'replace', index = False, dtype=dtypeMap, schema = 'public')
    #db.conn.commit()
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


    