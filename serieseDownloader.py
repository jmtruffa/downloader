import os
import tempfile
import pandas as pd
import requests
from dataBaseConn import DatabaseConnection


def download():
    url = "https://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/series.xlsm"

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
            print("File downloaded successfully")
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while downloading the file: {e}")
        return False

    return file_path

def bm(file_path = None):

    if file_path == None:

        file_path = download()

    # Read the specified range "A:AF" from the "BASE MONETARIA" sheet, skip first 8 rows
    data_df = pd.read_excel(file_path, sheet_name="BASE MONETARIA", usecols="A:AF", skiprows=8)
    
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
        columnDefinitionsSQL = ", ".join([f"{col} INTEGER" if col == "date" else f"{col} REAL" for col in column_definitions])
        

        db.create_table("bmBCRA", columnDefinitionsSQL)

    
    data_to_insert = data_df.to_dict(orient="records")

    db.insert_data_many("bmBCRA", data_to_insert, overwrite=True)
    
    db.disconnect()

    # Delete the temporary file if it was not passed as an argument
    if file_path == None:
        os.remove(file_path)

    return True

def reservas(file_path = None):

    if file_path == None:

        file_path = download()

    data_df = pd.read_excel(file_path, sheet_name="RESERVAS", usecols="A:Q", skiprows=9, header=None)

    columns_to_drop = [1, 5, 12, 14]
    data_df = data_df.drop(columns=data_df.columns[columns_to_drop])

    column_definitions = ("date",
      "stockTotal",
      "stockOroColPlazoOtros",
      "stockDivisasPasePasivoUSDExterior",
      "vdReservasIntl",
      "vdFeCompraDivisas",
      "vdFeOrgIntl",
      "vdFeOtrasOpSP",
      "vdFeEfecMinimo",
      "vdFeOtros",
      "AsigDEGs",
      "TC",
      "tipoSerie")
    
    data_df.columns = column_definitions

    data_df["date"] = pd.to_datetime(data_df["date"]).view('int64') // 10**9

    db = DatabaseConnection("/Users/juan/data/dataBCRA.sqlite3")
    db.connect()

    # Check if the table exists
    if not db.execute_select_query("SELECT name FROM sqlite_master WHERE type='table' AND name='reservas'"):
        # Define column names and types
        #column_definitions = ", ".join([f"{col} INTEGER" if col == "date" else f"{col} REAL" for col in data_df.columns])
        #column_definitions = cleanNames(data_df).dtypes.to_dict()
        

        columnDefinitionsSQL = ", ".join([f"{col} INTEGER" if col == "date" else f"{col} REAL" for col in column_definitions])
        

        db.create_table("reservas", columnDefinitionsSQL)

    db.insert_data_many("reservas", data_df, overwrite=True)
    
    db.disconnect()

    # Delete the temporary file if it was not passed as an argument
    if file_path == None:
        os.remove(file_path)

    return True

def depositos(file_path = None):

    if file_path == None:

        file_path = download()

    data_df = pd.read_excel(file_path, sheet_name="DEPOSITOS", usecols="A:AD", skiprows=9, header=None)

    columns_to_drop = [21, 24, 27]
    data_df = data_df.drop(columns=data_df.columns[columns_to_drop])

    column_definitions = (
      "date",
      "ptCtaCte",
      "ptCA",
      "ptPFNoAjust",
      "ptPFAjustCERUVA",
      "ptOtros",
      "ptCedrosCER",
      "ptTotalDepositos",
      "ptBodenContabilizado",
      "ptTotal",
      "pSPPesosCtaCte",
      "pSPPesosCA",
      "pSPPesosPFNoAjust",
      "pSPPesosPFAjustCERUVA",
      "pSPPesosOtros",
      "pSPPesosCedrosCER",
      "pSPPesosTotalDepositos",
      "pSPPesosBodenContabilizado",
      "pSPPesosTotal",
      "depositosDolaresExprPesosTotal",
      "depositosDolaresExprPesosSPrivado",
      "depositosTotales",
      "depositosTotalesSectorPrivado",
      "depositosDolaresExprDolaresTotal",
      "depositosDolaresExprDolaresSPrivado",
      "M2",
      "tipoSerie"
    )

    data_df.columns = column_definitions

    data_df["date"] = pd.to_datetime(data_df["date"]).view('int64') // 10**9

    db = DatabaseConnection("/Users/juan/data/dataBCRA.sqlite3")
    db.connect()

    # Check if the table exists
    if not db.execute_select_query("SELECT name FROM sqlite_master WHERE type='table' AND name='depositos'"):
        # Define column names and types
        columnDefinitionsSQL = ", ".join([f"{col} INTEGER" if col == "date" else f"{col} REAL" for col in column_definitions])
        
        db.create_table("depositos", columnDefinitionsSQL)

    db.insert_data_many("depositos", data_df, overwrite=True)
    
    db.disconnect()

    # Delete the temporary file if it was not passed as an argument
    if file_path == None:
        os.remove(file_path)

    return True

def prestamos(file_path = None):

    if file_path == None:

        file_path = download()

    data_df = pd.read_excel(file_path, sheet_name="PRESTAMOS", usecols="A:V", skiprows=9, header=None)

    columns_to_drop = [17, 19]
    data_df = data_df.drop(columns=data_df.columns[columns_to_drop])

    column_definitions = (
      "date",
      "prestamosSPPesosAdelantos",
      "prestamosSPPesosDocumentos",
      "prestamosSPPesosHipotecarios",
      "prestamosSPPesosPrendarios",
      "prestamosSPPesosPersonales",
      "prestamosSPPesosTarjetas",
      "prestamosSPPesosOtros",
      "prestamosSPPesosTotal",
      "prestamosSPDolaresAdelantos",
      "prestamosSPDolaresDocumentos",
      "prestamosSPDolaresHipotecarios",
      "prestamosSPDolaresPrendarios",
      "prestamosSPDolaresPersonales",
      "prestamosSPDolaresTarjetas",
      "prestamosSPDolaresOtros",
      "prestamosSPDolaresTotal",
      "prestamosSPMillonesPesosDolares",
      "prestamosSPPesosMasDolares",
      "tipoSerie"
    )

    data_df.columns = column_definitions

    data_df["date"] = pd.to_datetime(data_df["date"]).view('int64') // 10**9

    db = DatabaseConnection("/Users/juan/data/dataBCRA.sqlite3")
    db.connect()

    # Check if the table exists
    if not db.execute_select_query("SELECT name FROM sqlite_master WHERE type='table' AND name='prestamos'"):
        # Define column names and types
        columnDefinitionsSQL = ", ".join([f"{col} INTEGER" if col == "date" else f"{col} REAL" for col in column_definitions])
        
        db.create_table("prestamos", columnDefinitionsSQL)

    db.insert_data_many("prestamos", data_df, overwrite=True)

    db.disconnect()

    # Delete the temporary file if it was not passed as an argument
    if file_path == None:
        os.remove(file_path)

    return True

def tasas(file_path = None):

    if file_path == None:

        file_path = download() 

    data_df = pd.read_excel(file_path, sheet_name="TASAS DE MERCADO", usecols="A:V", skiprows=9, header=None)

    column_definitions = (
      "date",
      "PF3044DiasPesosTotalGeneralTNA",
      "PF3044DiasPesosHastaCienmilTNA",
      "PF3044DiasPesosHastaCienmilTEA",
      "PF3044DiasPesosMasUnmillonTNA",
      "PF3044DiasDolaresTotalGeneralTNA",
      "PF3044DiasDolaresHastaCienmilTNA",
      "PF3044DiasDolaresMasUnmillonTNA",
      "badlarPesosTotalTNA",
      "badlarPesosTotalBancosPrivadosTNA",
      "badlarPesosTotalBancosPrivadosTEA",
      "TM20PesosTotalTNA",
      "TM20PesosBancoprivadosTNA",
      "TM20PesosBancoprivadosTEA",
      "prestamosPersonalesPesosTotalTNA",
      "adelantosPesosTotalTNA",
      "callPesosEntreprivadosTasaTNA",
      "callPesosEntreprivadosMontoMillones",
      "callPesosTotalTasaTNA",
      "callPesosTotalMontoMillones",
      "pasesEntreTerceros1DiaTNA",
      "pasesEntreTercerosMontoMillones"
    )

    data_df.columns = column_definitions

    data_df["date"] = pd.to_datetime(data_df["date"]).view('int64') // 10**9

    db = DatabaseConnection("/Users/juan/data/dataBCRA.sqlite3")
    db.connect()

    # Check if the table exists
    if not db.execute_select_query("SELECT name FROM sqlite_master WHERE type='table' AND name='tasas'"):
        # Define column names and types
        columnDefinitionsSQL = ", ".join([f"{col} INTEGER" if col == "date" else f"{col} REAL" for col in column_definitions])
        
        db.create_table("tasas", columnDefinitionsSQL)

    db.insert_data_many("tasas", data_df, overwrite=True)

    db.disconnect()

    # Delete the temporary file if it was not passed as an argument
    if file_path == None:
        os.remove(file_path)

    return True

def instrumentos(file_path = None):

    if file_path == None:

            file_path = download() 

    data_df = pd.read_excel(file_path, sheet_name="INSTRUMENTOS DEL BCRA", usecols="A:AS", skiprows=9, header=None)
    
    column_definitions = (
      "date",
      "saldosPasesPasivosPesosTotal",
      "saldosPasesPasivosPesosFCI",
      "saldosPasesActivosPesos",
      "saldosLeliqNotaliq",
      "saldosLebacNobacPesosLegarLeminTotal",
      "saldosLebacNobacPesosLegarLeminEntFinancieras",
      "saldosLebacDolaresLediv",
      "saldosNocom",
      "tasaPolMonTNA",
      "tasaPolMonTEA",
      "tasaPasePesosPasivo1Dia",
      "tasaPasePesosPasivo7Dias",
      "tasaPasePesosActivo1Dia",
      "tasaPasePesosActivo7Dia",
      "tasaLebacPesosLeliq1M",
      "tasaLebacPesosLeliq2M",
      "tasaLebacPesosLeliq3M",
      "tasaLebacPesosLeliq4M",
      "tasaLebacPesosLeliq5M",
      "tasaLebacPesosLeliq6M",
      "tasaLebacPesosLeliq7M",
      "tasaLebacPesosLeliq8M",
      "tasaLebacPesosLeliq9M",
      "tasaLebacPesosLeliq10M",
      "tasaLebacPesosLeliq11M",
      "tasaLebacPesosLeliq12M",
      "tasaLebacPesosLeliq18M",
      "tasaLebacPesosLeliq24M",
      "tasaPesosCER6M",
      "tasaPesosCER12M",
      "tasaPesosCER18M",
      "tasaPesosCER24M",
      "tasaLebacDolar1MLiquidablePesos",
      "tasaLebacDolar6MLiquidablePesos",
      "tasaLebacDolar12MLiquidablePesos",
      "tasaLebacDolar1MLiquidableDolar",
      "tasaLebacDolar3MLiquidableDolar",
      "tasaLebacDolar6MLiquidableDolar",
      "tasaLebacDolar12MLiquidableDolar",
      "tasaNobacPesosVariableBadlarBcoPriv9M",
      "tasaNobacPesosVariableBadlarBcoPriv1A",
      "tasaNobacPesosVariableBadlarTotal2A",
      "tasaNobacPesosVariableBadlarBcoPriv2A",
      "tasaNotaliqPesosVariableTasaPolMon190d"
    )

    data_df.columns = column_definitions

    data_df["date"] = pd.to_datetime(data_df["date"]).view('int64') // 10**9

    db = DatabaseConnection("/Users/juan/data/dataBCRA.sqlite3")
    db.connect()
    
    # Check if the table exists
    if not db.execute_select_query("SELECT name FROM sqlite_master WHERE type='table' AND name='instrumentos'"):
        # Define column names and types
        columnDefinitionsSQL = ", ".join([f"{col} INTEGER" if col == "date" else f"{col} REAL" for col in column_definitions])
        
        db.create_table("instrumentos", columnDefinitionsSQL)
    
    db.insert_data_many("instrumentos", data_df, overwrite=True)

    db.disconnect()

    # Delete the temporary file if it was not passed as an argument
    if file_path == None:
        os.remove(file_path)
    
    return True

def main():
    file_path = download()
    for func in [bm, reservas, depositos, prestamos, tasas, instrumentos]:
        func(file_path)
        if func(file_path):
            print(f"{func.__name__} downloaded successfully")
        else:
            print(f"An error occurred while downloading {func.__name__}")
    os.remove(file_path)
    print("Temporary file deleted.")


# Example usage
if __name__ == "__main__":
    file_path = download()
    for func in [bm, reservas, depositos, prestamos, tasas, instrumentos]:
        func(file_path)
        if func(file_path):
            print(f"{func.__name__} downloaded successfully")
        else:
            print(f"An error occurred while downloading {func.__name__}")
    os.remove(file_path)
    print("Temporary file deleted.")
