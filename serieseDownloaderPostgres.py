import os
import tempfile
import pandas as pd
import requests
from urllib3.exceptions import InsecureRequestWarning
#from dataBaseConn2 import DatabaseConnection
import sqlalchemy
from datetime import datetime
from sqlalchemy import create_engine, text

db_user = os.environ.get('POSTGRES_USER')
db_password = os.environ.get('POSTGRES_PASSWORD')
db_host = os.environ.get('POSTGRES_HOST')
db_port = os.environ.get('POSTGRES_PORT', '5432')  # Default port for PostgreSQL is 5432
db_name = os.environ.get('POSTGRES_DB')
dtypeMap = {'date': sqlalchemy.types.Date}

def varAgregados():
    
    ag = pd.read_sql_query('SELECT * FROM "agregadosPrivados"', con=engine)

    

    ag['date'] = ag['date'].astype('datetime64[ns]')
        
    ag['anioMenos'] = ag['date'] - pd.DateOffset(years=1)

    # cast anioMenos to datetime
    ag['anioMenos'] = ag['anioMenos'].astype('datetime64[ns]')

    # merge df with itself on date and anioMenos

    ag = ag.merge(ag, left_on='anioMenos', right_on='date', suffixes=('', '_anioMenos'))
    ag.drop(columns=['anioMenos', 'date_anioMenos', 'anioMenos_anioMenos'], inplace=True)
    # divide columns 1:4 by columns 5:8
    for column in ag.columns[1:5]:
        ag[column] = ag[column] / ag[column + '_anioMenos'] - 1
    
    # drop columns 5:8
    ag.drop(columns=ag.columns[5:9], inplace=True)

    dtypeMap = {'date': sqlalchemy.types.Date}

    ag.to_sql(name='varAgregados', con=engine, if_exists='replace', index=False, schema='public', dtype=dtypeMap)
    # commit
    #db.conn.commit()

    return ag


def download():
    url = "https://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/series.xlsm"

    # Create a temporary directory to store the downloaded file
    temp_dir = tempfile.mkdtemp()

    # File path for the downloaded XLSM file
    file_path = os.path.join(temp_dir, "data.xlsm")

    # Download the XLS file from the URL
    try:
        requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
        response = requests.get(url, verify=False) # The server's SSL certificate is not verified. This should not be used in production.
        response.raise_for_status()  # Check if the request was successful
        with open(file_path, "wb") as file:
            file.write(response.content)
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print("------------------------------------")
            print(f"File downloaded successfully at {current_time}")
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while downloading the file: {e}")
        return False

    return file_path

def bm(file_path = None):

    if file_path == None:

        file_path = download()

    # Read the specified range "A:AF" from the "BASE MONETARIA" sheet, skip first 8 rows
    data_df = pd.read_excel(file_path, sheet_name="BASE MONETARIA", usecols="A:AG", skiprows=8)
    
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
      "vdFeOperacionesLEFI",
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
    
    data_df.to_sql('bmBCRA', engine, if_exists='replace', index=False, dtype=dtypeMap)


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

    data_df.to_sql('reservas', engine, if_exists='replace', index=False, dtype=dtypeMap)
    

    # Delete the temporary file if it was not passed as an argument
    if file_path == None:
        os.remove(file_path)

    return True

def depositos(file_path = None):

    if file_path == None:

        file_path = download()

    data_df = pd.read_excel(file_path, sheet_name="DEPOSITOS", usecols="A:AE", skiprows=9, header=None)

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
      "M2_transac_privado",
      "tipoSerie"
    )

    data_df.columns = column_definitions

    data_df.to_sql('depositos', engine, if_exists='replace', index=False, dtype=dtypeMap)
    with engine.connect() as con:
        con.execute(text("CALL agregadosprivados();"))
    #engine.connect.execute("CALL agregadosprivados();") ##db.execute_query("SELECT agregadosprivados();")##

    # acá vamos a calcular las variaciones anuales de los agregados privados
    df_varAg = varAgregados()

    df_varAg.to_sql('varAnualAgregadosPrivados', engine, if_exists='replace', index=False, dtype=dtypeMap)
    
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

    data_df.to_sql('prestamos', engine, if_exists='replace', index=False, dtype=dtypeMap)

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

    data_df.to_sql('tasas', engine, if_exists='replace', index=False, dtype=dtypeMap)

    # Delete the temporary file if it was not passed as an argument
    if file_path == None:
        os.remove(file_path)

    return True

def instrumentos(file_path = None):

    if file_path == None:

            file_path = download() 

    data_df = pd.read_excel(file_path, sheet_name="INSTRUMENTOS DEL BCRA", usecols="A:AU", skiprows=9, header=None)
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
      "tasaNotaliqPesosVariableTasaPolMon190d",
      "vacio",
      "saldolefi"
    )

    data_df.columns = column_definitions

    # drop columna "vacio"
    data_df = data_df.drop(columns=["vacio"])

    data_df.to_sql('instrumentos', engine, if_exists='replace', index=False, dtype=dtypeMap)
    

    # Delete the temporary file if it was not passed as an argument
    if file_path == None:
        os.remove(file_path)
    
    return True


# Example usage
if __name__ == "__main__":
    file_path = download()

    engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
    #db = DatabaseConnection(db_type='postgresql', db_name=os.environ.get('POSTGRES_DB'))
    #db.connect()
    # use Date type for the 'date' column in the database to get rid of the time part
    
    for func in [bm, reservas, depositos, prestamos, tasas, instrumentos]:
        if func(file_path):
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"{func.__name__} parsed successfully at {current_time}")
        else:
            print(f"An error occurred while downloading {func.__name__}")
    os.remove(file_path)
    #db.disconnect()
    print("Temporary file deleted.")
