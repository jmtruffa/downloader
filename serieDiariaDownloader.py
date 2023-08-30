import os
import tempfile
import pandas as pd
import requests
import datetime
from dataBaseConn import DatabaseConnection


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
        response = requests.get(url)
        response.raise_for_status()  # Check if the request was successful
        with open(file_path, "wb") as file:
            file.write(response.content)
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while downloading the file: {e}")
        return False

    return file_path


def serieDiaria(file_path = None):

    if file_path == None:
        file_path = download()

    # Read the XLSM file
    data_df = pd.read_excel(file_path, sheet_name="Serie_diaria", skiprows=26, usecols="A:AG", header=None)

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
    "princPasSaldosPESOSDepGobierno",
    "redescuentosAdelantos",
    "tipoCambio"
    )

    data_df.columns = column_definitions

    # Convert the date column to unix timestamp
    data_df["date"] = pd.to_datetime(data_df["date"], format="%Y%m%d").view('int64') // 10**9
    #data_df["date"] = pd.to_datetime(data_df["date"]).to_timestamp

    db = DatabaseConnection("/Users/juan/data/dataBCRA.sqlite3")
    db.connect()

    # Check if the table exists
    if not db.execute_select_query("SELECT name FROM sqlite_master WHERE type='table' AND name='serieDiaria'"):
        
        # Define column names and types        
        columnDefinitionsSQL = ", ".join([f"{col} INTEGER" if col == "date" else f"{col} REAL" for col in column_definitions])
        

        db.create_table("serieDiaria", columnDefinitionsSQL)

    data_to_insert = data_df.to_dict(orient="records")

    db.insert_data_many("serieDiaria", data_to_insert, overwrite=True)
    
    db.disconnect()

    # Delete the temporary file if it was not passed as an argument
    if file_path == None:
        os.remove(file_path)

    return True
    
if __name__ == "__main__":
    file_path = download()
    serieDiaria(file_path)
    os.remove(file_path)

