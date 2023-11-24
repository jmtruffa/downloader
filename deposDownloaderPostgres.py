import os
import tempfile
import pandas as pd
import requests
import datetime
from dataBaseConn2 import DatabaseConnection
import sys
import sqlalchemy

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
        response = requests.get(url)
        response.raise_for_status()  # Check if the request was successful
        with open(file_path, "wb") as file:
            file.write(response.content)
            current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
    #convert to string
    data_df['date'] = data_df['date'].astype(str)

    # Pre-process the 'date' column to remove '.0' DepPrivados gets that .0
    data_df['date'] = data_df['date'].str.rstrip('.0')

    data_df.dropna(subset=[data_df.columns[1]], inplace=True)

    date_format = "%Y%m%d"
        
    data_df['date'] = pd.to_datetime(data_df['date'], format=date_format)
  

    # Check if the table exists. If not, create it.
    table_exists_query = f"""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public' AND table_name = '{aTableName}'
    """
    result = pd.read_sql(table_exists_query, db.conn)


    if result.empty:
        data_to_insert = data_df 
    else:
        query = f'SELECT MAX(date) FROM "{aTableName}"'
        last_date = pd.read_sql(query, db.conn).iloc[0,0]
        # convert to datetime in order to compare to the date column in data_df
        last_date = pd.to_datetime(last_date)
        
        data_to_insert = data_df[data_df['date'] > last_date]

    if len(data_to_insert) == 0:
        print("No rows to be inserted. Exiting...")
    else:
        print(f"Inserting {len(data_to_insert)} rows into {aTableName}")
        # use Date type for the 'date' column in the database to get rid of the time part
        dtypeMap = {'date': sqlalchemy.types.Date}
        result = data_to_insert.to_sql(name = aTableName, con = db.engine, if_exists = 'append', index = False, dtype=dtypeMap, schema = 'public')
        db.conn.commit()
        print(f"Number of records inserted as reported by the postgres server: {result}") 


    if file_path == None:
        os.remove(file_path)

    return True

    
if __name__ == "__main__":
    if len(sys.argv) > 1:
        # Pidió año para bajar
        year = sys.argv[1]
        print(year)
    else:
        # no pidió. Se asume el año actual
        year = str(2022) #str(datetime.date.today().year)
    
    print(f"Descargando serie de depositos para el año {year}")

    file_path = download(year = year)

    if file_path == False:
        exit()
    
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

    db = DatabaseConnection(db_type='postgresql', db_name=os.environ.get('POSTGRES_DB'))
    db.connect()
    for index, row in df.iterrows():
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print("------------------------------------")
        print(f"Procesando {row['aSheet'], row['aTableName']} at {current_time}")
        parseSheets(row["aSheet"], row["aSkipRows"], row["aUseCols"], row["aTableName"], row["aColumnsToDrop"], file_path)

    db.disconnect()
    os.remove(file_path)


    

