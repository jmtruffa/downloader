import tempfile
import os
import requests
from datetime import datetime
import pandas as pd
from dataBaseConn2 import DatabaseConnection
import sqlalchemy


def downloadIPC(aCurrentTime):
    # guardo el mes en la variable mes. 
    mes = datetime.now().month

    # guardo el año en la variable anio
    anio = datetime.now().year

    # tomo los últimos dos caracteres del año
    anio = str(anio)[2:]

    # construyo url "https://www.indec.gob.ar/ftp/cuadros/economia/sh_ipc_11_23.xls" con el mes actual
    url = "https://www.indec.gob.ar/ftp/cuadros/economia/sh_ipc_" + str(mes) + "_" + anio + ".xls"
    print(url)

   
    # Create a temporary directory to store the downloaded file
    temp_dir = tempfile.mkdtemp()

    # File path for the downloaded XLS file
    file_path = os.path.join(temp_dir, "ipc.xls")

    # Download the XLS file from the URL 
    try:
        response = requests.get(url)
        response.raise_for_status()  # Check if the request was successful
        with open(file_path, "wb") as file:
            file.write(response.content)
    except requests.exceptions.RequestException as e:
        print(f"No se pudo descargar el archivo: {e}")
        return False
    
    currentTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("------------------------------------")
    print(f"IPC descargado OK a las {aCurrentTime}")

    return file_path

def parseIPC(file_path, aCurrentTime):
    print(f"Parseando el archivo IPC. Iniciado a las {aCurrentTime} ")

    # Read the XLSM file
    data_df = pd.read_excel(file_path, header=None, skiprows=5, sheet_name=2)

    # transpose the DataFrame
    data_df = data_df.T

    # Drop columns
    columns_to_drop = [1, 2, 3, 17, 18, 22, 23] + list(range(26, 34,1)) + [47, 48, 52, 53] + list(range(56, 64, 1)) + [77, 78, 82, 83] + list(range(86,94,1)) + [107, 108, 112, 113] + list(range(116,124,1)) + [137, 138, 142, 143] + list(range(146,154,1)) + [167, 168, 172, 173] + list(range(176,184,1)) + [197, 198, 202, 203] + list(range(206,211,1))
    data_df = data_df.drop(columns=data_df.columns[columns_to_drop])
    
    # drop row 0
    data_df = data_df.drop([0])

    # Set column names
    data_df.columns = ["date", 
                       "nacionalNivelGeneral", "nacionalAlimBebidasNoAlcohol", "nacionalBebidasAlcoholTabaco", "nacionalPrendasVestirCalzado", "nacionalViviendaAgua", "nacionalEquipamiento", "nacionalSalud", "nacionalTransporte", "nacionalComunicacion", "nacionalRecreacion", "nacionalEducacion", "nacionalRestaurant", "nacionalBsSvsVarios", "nacionalEstacional", "nacionalNucleo", "nacionalRegulados", "nacionalBienes", "nacionalServicios",
                       "gbaNivelGeneral", "gbaAlimBebidasNoAlcohol", "gbaBebidasAlcoholTabaco", "gbaPrendasVestirCalzado", "gbaViviendaAgua", "gbaEquipamiento", "gbaSalud", "gbaTransporte", "gbaComunicacion", "gbaRecreacion", "gbaEducacion", "gbaRestaurant", "gbaBsSvsVarios", "gbaEstacional", "gbaNucleo", "gbaRegulados", "gbaBienes", "gbaServicios",
                       "pampeanaNivelGeneral", "pampeanaAlimBebidasNoAlcohol", "pampeanaBebidasAlcoholTabaco", "pampeanaPrendasVestirCalzado", "pampeanaViviendaAgua", "pampeanaEquipamiento", "pampeanaSalud", "pampeanaTransporte", "pampeanaComunicacion", "pampeanaRecreacion", "pampeanaEducacion", "pampeanaRestaurant", "pampeanaBsSvsVarios", "pampeanaEstacional", "pampeanaNucleo", "pampeanaRegulados", "pampeanaBienes", "pampeanaServicios",
                       "noaNivelGeneral", "noaAlimBebidasNoAlcohol", "noaBebidasAlcoholTabaco", "noaPrendasVestirCalzado", "noaViviendaAgua", "noaEquipamiento", "noaSalud", "noaTransporte", "noaComunicacion", "noaRecreacion", "noaEducacion", "noaRestaurant", "noaBsSvsVarios", "noaEstacional", "noaNucleo", "noaRegulados", "noaBienes", "noaServicios",
                          "neaNivelGeneral", "neaAlimBebidasNoAlcohol", "neaBebidasAlcoholTabaco", "neaPrendasVestirCalzado", "neaViviendaAgua", "neaEquipamiento", "neaSalud", "neaTransporte", "neaComunicacion", "neaRecreacion", "neaEducacion", "neaRestaurant", "neaBsSvsVarios", "neaEstacional", "neaNucleo", "neaRegulados", "neaBienes", "neaServicios",
                            "cuyoNivelGeneral", "cuyoAlimBebidasNoAlcohol", "cuyoBebidasAlcoholTabaco", "cuyoPrendasVestirCalzado", "cuyoViviendaAgua", "cuyoEquipamiento", "cuyoSalud", "cuyoTransporte", "cuyoComunicacion", "cuyoRecreacion", "cuyoEducacion", "cuyoRestaurant", "cuyoBsSvsVarios", "cuyoEstacional", "cuyoNucleo", "cuyoRegulados", "cuyoBienes", "cuyoServicios",
                            "patagoniaNivelGeneral", "patagoniaAlimBebidasNoAlcohol", "patagoniaBebidasAlcoholTabaco", "patagoniaPrendasVestirCalzado", "patagoniaViviendaAgua", "patagoniaEquipamiento", "patagoniaSalud", "patagoniaTransporte", "patagoniaComunicacion", "patagoniaRecreacion", "patagoniaEducacion", "patagoniaRestaurant", "patagoniaBsSvsVarios", "patagoniaEstacional", "patagoniaNucleo", "patagoniaRegulados", "patagoniaBienes", "patagoniaServicios"
    ]


    # date column without time
    data_df["date"] = pd.to_datetime(data_df["date"]).dt.date

    # convert all columns except date to numeric
    for column in data_df.columns[1:]:
        data_df[column] = pd.to_numeric(data_df[column], errors='coerce')

    data_dfVar = data_df.copy()
    data_dfVar.columns = ["date"] + ["var" + column[0].upper() + column[1:] for column in data_df.columns[1:]]

    # for every column except date, calculate the percentage change from the previous row
    for column in data_dfVar.columns[1:]:
        data_dfVar[column] = data_dfVar[column].pct_change()

    # drop the first column of data_dfVar and bind rows with data_df
    #data_df = pd.concat([data_df.iloc[:, 0], data_dfVar.iloc[:, 1:]], axis=1)
    
    data_df = pd.concat([data_df.iloc[:,:], data_dfVar.iloc[:, 1:]], axis=1)

    return data_df

def saveIPC(df, aCurrentTime):
    """Insert the data into the database"""

    print(f"Grabando el IPC en la base de datos. Iniciado a las {aCurrentTime} ")

    # connect to the database
    db = DatabaseConnection(db_type="postgresql", db_name=os.environ.get('POSTGRES_DB'))
    db.connect()

    # Check if there are rows to be inserted
    if len(df) == 0:
        print("No hay datos para insertar. Saliendo...")
    else:
        print(f"Insertando {len(df)} filas en la tabla IPC")
        # use Date type for the 'date' column in the database to get rid of the time part
        dtypeMap = {'date': sqlalchemy.types.Date}
        result = df.to_sql(name = 'IPCIndec', con = db.engine, if_exists = 'replace', index = False, dtype=dtypeMap, schema = 'public')
        db.conn.commit()
        print(f"Number of records inserted as reported by the postgres server: {result}") 

    
    db.disconnect()

    print(f"IPC grabado OK a las {aCurrentTime} ")

    return True



def main():
    currentTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # bajamos el ipc. Me devuelve el path del archivo
    file_path = downloadIPC(currentTime)

    # parseamos el ipc
    if file_path:
        currentTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df = parseIPC(file_path, currentTime)

    os.remove(file_path)
    
    # grabamos el ipc en la base de datos
    if df is not None:
        currentTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        saveIPC(df, currentTime)
    
    return True

if __name__ == "__main__":
    main()
    

