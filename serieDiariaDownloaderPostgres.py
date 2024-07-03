import os
import tempfile
import pandas as pd
import requests
from urllib3.exceptions import InsecureRequestWarning
import datetime
from dataBaseConn2 import DatabaseConnection
import sys
import sqlalchemy


def download(year = str(datetime.date.today().year)):
    """ Download the XLSM file from the BCRA website and return the file path """
    """ The default year is the current year """

    url = "http://www.bcra.gov.ar/Pdfs/PublicacionesEstadisticas/bas"+year+".xls"

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
            print(f"File for the year: {year}, downloaded successfully at {current_time}")            
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while downloading the file: {e}")
        return False

    return file_path


def parseSerieDiaria(year, file_path = None):
    """ Parse the specified sheet from the downloaded XLSM file and insert the data into the database"""

    if file_path == None:
        file_path = download()

    # Read the XLSM file
    data_df = pd.read_excel(file_path, sheet_name="Serie_diaria", skiprows=26, usecols="A:AI", header=None)

    columns_to_drop = [0, 1, 6]
    data_df = data_df.drop(columns=data_df.columns[columns_to_drop])

    # Rename the columns
    column_definitions = (
        "date",
    "reservasIntSaldosUSDTotal",
    "reservasIntSaldosUSDOroDivColocOtros",
    "reservasIntSaldosUSDDividsasPasePasivoUSDExterior",
    "princPasSaldosPESOSPasMonetariosTotal",
    "princPasSaldosPESOSPasMonetariosBMTotal",
    "princPasSaldosPESOSPasMonetariosBMCMTotal",
    "princPasSaldosPESOSPasMonetariosBMCMChequesCancel",
    "princPasSaldosPESOSPasMonetariosBMCtaCteTotal",
    "princPasSaldosPESOSPasMonetariosBMCtaCteFondosPagoOPP",
    "princPasSaldosPESOSPasMonetariosBMCtaCteResto",
    "princPasSaldosPESOSPasMonetariosDepositosMonExtTotal",
    "princPasSaldosPESOSPasMonetariosDepositosMonExtDepCtaCte",
    "princPasSaldosPESOSPasMonetariosDepositosMonExtCEDINCheqCanc",
    "princPasSaldosPESOSLebacNobacMonedaNacionalSaldo",
    "princPasSaldosPESOSLebacNobacMonedaNacionalMontoEfectivoColocado",
    "princPasSaldosPESOSLebacNobacMonedaNacionalMontoVto",
    "princPasSaldosPESOSLebacNobacUSDSaldo",
    "princPasSaldosPESOSLebacNobacUSDMontoEfectivoColocado",
    "princPasSaldosPESOSLebacNobacUSDMontoVto",
    "princPasSaldosPESOSLeliqNotaliqMonedaNacionalSaldo",
    "princPasSaldosPESOSLeliqNotaliqMonedaNacionalMontoEfectivoColocado",
    "princPasSaldosPESOSLeliqNotaliqMonedaNacionalMontoVto",
    "princPasSaldosPESOSNOCOM",
    "princPasSaldosPESOSPosNetaPasesTotal",
    "princPasSaldosPESOSPosNetaPasesPasesPasivos",
    "princPasSaldosPESOSPosNetaPasesPasesActivos",
    "princPasSaldosTOTALDepGobierno",
    "princPasSaldosPESOSDepGobierno",
    "princPasSaldosMEDepGobierno",
    "redescuentosAdelantos",
    "tipoCambio"
    )

    data_df.columns = column_definitions

    data_df.dropna(subset=[data_df.columns[0]], inplace=True)
    
    date_format = "%Y%m%d"
    data_df['date'] = pd.to_datetime(data_df['date'], format=date_format)

    # if year < than current year, eliminate rows greater than month 12
    if data_df.loc[0,'date'].year < datetime.date.today().year:
         # eliminate rows greater than month 12 of current year
        dateAComparar = pd.to_datetime(datetime.date(data_df.loc[0, 'date'].year, 12, 31))
        data_df = data_df[data_df['date'] <= dateAComparar]
    
    db = DatabaseConnection(db_type='postgresql', db_name=os.environ.get('POSTGRES_DB'))
    db.connect()
    dtypeMap = {'date': sqlalchemy.types.Date}
    

    # ya que los datos van cambiando para las fechas poteriores a la actual
    # tengo que grabar todo el año completo
    # Para eso tengo que borrar los datos del año actual hacia adelante
    # Borrar todos los datos desde el 1 de enero del año en curso hacia adelante
    if year == str(datetime.date.today().year):
         
        fecha = datetime.datetime(datetime.datetime.now().year, 1, 1).strftime("%Y-%m-%d")
 
        query = f"DELETE FROM \"serieDiaria\" WHERE date >= '{fecha}'"
        print("Borrando datos del año en curso...")
        
        # Wrapping the query in a text() function allows SQLAlchemy to correctly run it
        db.execute_query((query))
        #db.conn.execute(sqlalchemy.text(query)) 
        
    print(f"Inserting or updating {len(data_df)} rows into serieDiaria")
    data_df.to_sql(name = "serieDiaria", con = db.conn, if_exists = 'append', index = False, dtype = dtypeMap)
    #db.conn.commit()
    
    if file_path == None:
        os.remove(file_path)

    db.disconnect()

    return True
    

    
if __name__ == "__main__":
    if len(sys.argv) > 1:
        year = sys.argv[1]
    else:
        year = str(datetime.date.today().year)

    print(f"Descargando serie diaria de BCRA...")

    file_path = download(year)

    parseSerieDiaria(file_path = file_path, year = year)

    print("Base de datos actualizada.")
    print("Borrando archivo temporal...")

    os.remove(file_path)

    

