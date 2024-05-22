import os
import sys
import tempfile
import pandas as pd
import requests
from urllib3.exceptions import InsecureRequestWarning
import datetime
from dataBaseConn import DatabaseConnection


def download(year = str(datetime.date.today().year)):
    """ Download the XLSM file from the BCRA website and return the file path """
    """ The default year is the current year """

    url = "https://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/dep"+year+".xls"

    # Create a temporary directory to store the downloaded file
    temp_dir = tempfile.mkdtemp()

    # File path for the downloaded XLSM file
    file_path = os.path.join(temp_dir, "data.xlsm")

    # Download the XLS file from the URL
    try:
        requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
        response = requests.get(url, verify=False)
        response.raise_for_status()  # Check if the request was successful
        with open(file_path, "wb") as file:
            file.write(response.content)
            current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print("------------------------------------")
            print(f"File for the year: {year}, downloaded successfully at {current_time}")            
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while downloading the file: {e}")
        return False

    return file_path

def parseSheets(aSheet, aSkipRows, aUseCols, aTableName, aColumnsToDrop, file_path = None):
    """ Parse the specified sheet from the downloaded XLSM file and insert the data into the database """
    if file_path == None:
        file_path = download()

    # Read the XLSM file
    data_df = pd.read_excel(file_path, sheet_name = aSheet, skiprows = aSkipRows, usecols = aUseCols)

    data_df = data_df.drop(columns=data_df.columns[aColumnsToDrop])
    
    # Rename the columns
    data_df = data_df.rename(columns={data_df.columns[0]: 'date'})
    #data_df.columns[0] = "date"

    # Convert the date column to unix timestamp
    data_df["date"] = pd.to_datetime(data_df["date"], format="%Y%m%d").view('int64') // 10**9

    # clean the df from some unwanted rows filled with -9223372037
    # don't know what's causing the issue
    # there are not missing values or malformed dates in the original file
    data_df = data_df[data_df['date'] != -9223372037] 

    # Initialize the database connection abstraction
    db = DatabaseConnection("/home/juant/data/dataBCRA.sqlite3")
    db.connect()

    # Check if the table exists. If not, create it.
    if not db.execute_select_query(f"SELECT name FROM sqlite_master WHERE type='table' AND name= '{aTableName}'"):
        
        # Define column names and types        
        columnDefinitionsSQL = ", ".join([f"{col} INTEGER" if col == "date" else f"{col} REAL" for col in data_df.columns])

        db.create_table(aTableName, columnDefinitionsSQL)

        data_to_insert = data_df.to_dict(orient="records")
    
    else:
        # Check if the table has data. If it does, get the last date in the table
        last_date = db.execute_select_query(f"SELECT MAX(date) FROM {aTableName}")[0][0]

        # Filter the dataframe to only include rows with dates greater than the last date in the table
        data_df = data_df[data_df["date"] > last_date]

        data_to_insert = data_df.to_dict(orient="records")

    # Check if there are rows to be inserted
    if len(data_to_insert) == 0:
        print("No rows to be inserted. Exiting...")
    
    else:
        print(f"Inserting {len(data_to_insert)} rows into {aTableName}")

        # Insert the data into the table
        db.insert_data_many(aTableName, data_to_insert, overwrite=False)
    
    db.disconnect()

    # Delete the temporary file if it was not passed as an argument
    if file_path == None:
        os.remove(file_path)

    return True



    
if __name__ == "__main__":
    print("Descargando serie de depositos...")
    file_path = download()

    if file_path == False:
        sys.exit()
    
    print("Serie diaria de depositos descargada. Actualizando base de datos...")


    params = {
    "aSheet": ["Total_sectores", 
               "Total_sectores", 
               "Sector_público",
               "Sector_privado",
               "UVAs",
               "DM20_total_sectores",
               "DM20_sector_publico",
               "DM20_sector_privado"],
    "aSkipRows": [25,
                  25,
                  25,
                  25,
                  25,
                  25,
                  25,
                  25],
    "aUseCols": [
        "A:EN",
        "EP:EV",
        "A:EN",
        "A:EN",
        "A:L",
        "A:AI",
        "A:AI",
        "A:AI"
        ],
    "aTableName": ["depTotalSectores",
                   "depTotalSectoresEntidadesNoInformantes",
                    "depSecPublico",
                   "depSecPrivado",
                    "depUVA",
                    "depDM20TotalSectores",
                    "depDM20SectorPublico",
                    "depDM20SectorPrivado"],
    "aColumnsToDrop": [
        [1, 2, 142],
        [],
        [1,2, 142],
        [1,2, 142],
        [1,2],
        [1,2,33],
        [1,2,33],
        [1,2,33]
    ]
    }

    df = pd.DataFrame(params)


    for index, row in df.iterrows():

        print(f"Procesando {row['aTableName']}")
        parseSheets(row["aSheet"], row["aSkipRows"], row["aUseCols"], row["aTableName"], row["aColumnsToDrop"], file_path)

    os.remove(file_path)


    

