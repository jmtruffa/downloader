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

def replaceTable(df, tableName):
    """Reemplaza el contenido de una tabla sin destruirla.

    No se usa to_sql(if_exists='replace'): eso emite un DROP TABLE sin CASCADE
    y Postgres lo rechaza cuando la tabla tiene objetos dependientes (una vista
    o un matview colgado de ella). TRUNCATE + append es transaccional en
    Postgres: preserva el esquema de la tabla, no rompe esas dependencias y
    ningún lector ve la tabla vacía a mitad de camino.

    Si la tabla todavía no existe se cae a 'replace', que en ese caso la crea.

    Contracara de no usar 'replace': el append no puede cambiar el esquema. Si
    la planilla del BCRA gana o pierde una columna, esto corta con un mensaje
    explícito en vez de un error opaco de psycopg, y hay que migrar la tabla a
    mano antes de volver a correr.
    """
    inspector = sqlalchemy.inspect(engine)
    tableExists = inspector.has_table(tableName, schema='public')

    if tableExists:
        enTabla = {c['name'] for c in inspector.get_columns(tableName, schema='public')}
        if enTabla != set(df.columns):
            raise RuntimeError(
                f'public."{tableName}": las columnas no coinciden con las de la tabla. '
                f'Sobran en el origen: {sorted(set(df.columns) - enTabla)}. '
                f'Faltan en el origen: {sorted(enTabla - set(df.columns))}. '
                'TRUNCATE + append no cambia el esquema: migrar la tabla '
                '(DROP si no tiene dependencias, o ALTER TABLE) y volver a correr.'
            )

    with engine.begin() as con:
        if tableExists:
            con.execute(text(f'TRUNCATE TABLE public."{tableName}"'))
        df.to_sql(
            name=tableName,
            con=con,
            if_exists='append' if tableExists else 'replace',
            index=False,
            schema='public',
            dtype=dtypeMap,
        )

def refreshAgregadosPrivados():
    """Refresca el matview public."agregadosPrivados" (agregados monetarios del
    sector privado, serie diaria, con sus variaciones anuales).

    Va al final del ETL, cuando depositos y bmBCRA ya están cargados y
    commiteados, y en conexión propia en AUTOCOMMIT porque REFRESH MATERIALIZED
    VIEW CONCURRENTLY no puede ejecutarse dentro de una transacción.

    Reemplaza al procedure agregadosprivados() (que dropeaba y recreaba la
    tabla, llevándose los índices y dejando a los lectores sin tabla mientras
    corría) y a la función varAgregados() en pandas, que traía la serie entera
    para dividir columnas y perdía el 30% de las fechas. La definición vive en
    agregadosPrivados.sql. Si el matview no existe se avisa y se sigue;
    cualquier otro error se propaga para que el MAILTO del cron avise.
    """
    conn = engine.connect().execution_options(isolation_level="AUTOCOMMIT")
    try:
        exists = conn.execute(text(
            "SELECT 1 FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = 'public' AND c.relname = 'agregadosPrivados' "
            "AND c.relkind = 'm'"
        )).scalar()

        if not exists:
            print('El matview public."agregadosPrivados" no existe: se omite el refresh.')
            return

        print('Refrescando public."agregadosPrivados"...')
        conn.execute(text(
            'REFRESH MATERIALIZED VIEW CONCURRENTLY public."agregadosPrivados"'
        ))
        print('public."agregadosPrivados" refrescado OK.')
    finally:
        conn.close()

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
    
    replaceTable(data_df, 'bmBCRA')


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

    replaceTable(data_df, 'reservas')
    

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

    replaceTable(data_df, 'depositos')

    # Los agregados privados salen del matview "agregadosPrivados", que se
    # refresca al final de main() — no acá, porque también depende de bmBCRA.

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

    replaceTable(data_df, 'prestamos')

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

    replaceTable(data_df, 'tasas')

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

    replaceTable(data_df, 'instrumentos')
    

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

    # Recién acá: el matview depende de depositos y de bmBCRA, así que se
    # refresca cuando las dos están cargadas, sin depender del orden de la lista.
    refreshAgregadosPrivados()

    os.remove(file_path)
    #db.disconnect()
    print("Temporary file deleted.")
