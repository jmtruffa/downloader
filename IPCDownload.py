import tempfile
import os
import sys
import time
import requests
from datetime import datetime
import pandas as pd
from dataBaseConn2 import DatabaseConnection
import sqlalchemy


# Firmas de archivo aceptadas. OLE2 es el .xls clasico que publica INDEC; ZIP cubre un
# eventual cambio a .xlsx, que pandas tambien lee.
FIRMAS_EXCEL = (b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1", b"PK\x03\x04")

# Reintentos de la descarga. Apenas INDEC publica (16:00) el host se satura y corta la
# transferencia a mitad de archivo: el 13/08/2026 trajo 2.170.549 de 2.372.608 bytes.
# Un reintento con espera resuelve ese corte sin esperar a la corrida del dia siguiente.
REINTENTOS = 3
ESPERA_REINTENTO_SEG = 20


def err(mensaje):
    """Escribe en stderr.

    El wrapper manda stdout al log y deja stderr libre a proposito, para que el MAILTO del
    cron avise. Todo lo que sea un fallo real va por aca; el relato normal va por print().
    """
    print(mensaje, file=sys.stderr)


def esExcel(contenido):
    """True si el contenido arranca con la firma de un Excel (OLE2 o ZIP/xlsx)."""
    return contenido[:8].startswith(FIRMAS_EXCEL)


def downloadIPC(aCurrentTime):
    # guardo el mes en la variable mes. 
    mes = datetime.now().month

    # guardo el año en la variable anio
    anio = datetime.now().year

    # tomo los últimos dos caracteres del año
    anio = str(anio)[2:]

    # construyo url "https://www.indec.gob.ar/ftp/cuadros/economia/sh_ipc_11_23.xls" con el mes actual
    # str(mes) with leading zero if necessary
    if mes < 10:
        mes = "0" + str(mes)
    else:
        mes = str(mes)
    url = "https://www.indec.gob.ar/ftp/cuadros/economia/sh_ipc_" + mes + "_" + anio + ".xls"
    print(url)
    # https://www.indec.gob.ar/ftp/cuadros/economia/sh_ipc_01_24.xls
   
    # Create a temporary directory to store the downloaded file
    temp_dir = tempfile.mkdtemp()

    # File path for the downloaded XLS file
    file_path = os.path.join(temp_dir, "ipc.xls")

    # Devuelve (estado, file_path) con estado en {"ok", "no_publicado", "error"}.
    #
    # La distincion importa: "todavia no publicaron" es el caso NORMAL los primeros dias de la
    # ventana y tiene que terminar en salida limpia, mientras que un fallo real tiene que
    # avisar. Antes no se distinguian: se guardaba cualquier respuesta como .xls y el problema
    # aparecia recien en pd.read_excel, como un ValueError que no dice nada
    # ("Excel file format cannot be determined"). Paso el 10 y el 12/08/2026.
    for intento in range(1, REINTENTOS + 1):
        try:
            response = requests.get(url, timeout=120)
        except requests.exceptions.RequestException as e:
            err(f"Intento {intento}/{REINTENTOS}: fallo la descarga: {e}")
            if intento < REINTENTOS:
                time.sleep(ESPERA_REINTENTO_SEG)
                continue
            return "error", None

        if response.status_code == 404:
            # El cuadro del mes todavia no esta subido. No es un fallo.
            print(f"INDEC todavia no publico {url} (HTTP 404).")
            return "no_publicado", None

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            err(f"Intento {intento}/{REINTENTOS}: {e}")
            if intento < REINTENTOS:
                time.sleep(ESPERA_REINTENTO_SEG)
                continue
            return "error", None

        contenido = response.content

        # Content-Length parcial = la transferencia se corto a mitad de archivo. Se reintenta:
        # el archivo esta bien del lado de INDEC, lo que fallo es el transporte.
        largoDeclarado = response.headers.get("Content-Length")
        if largoDeclarado is not None and len(contenido) != int(largoDeclarado):
            err(f"Intento {intento}/{REINTENTOS}: descarga incompleta "
                f"({len(contenido)} de {largoDeclarado} bytes).")
            if intento < REINTENTOS:
                time.sleep(ESPERA_REINTENTO_SEG)
                continue
            return "error", None

        # INDEC responde 200 con una pagina HTML cuando el cuadro todavia no esta, asi que el
        # status por si solo no alcanza: hay que mirar el contenido.
        if not esExcel(contenido):
            tipo = response.headers.get("Content-Type", "desconocido")
            print(f"La respuesta de {url} no es un Excel "
                  f"(Content-Type: {tipo}, {len(contenido)} bytes): se toma como no publicado.")
            return "no_publicado", None

        with open(file_path, "wb") as file:
            file.write(contenido)

        print("------------------------------------")
        print(f"IPC descargado OK a las {aCurrentTime} ({len(contenido)} bytes)")
        return "ok", file_path

    return "error", None

def parseIPC(file_path, aCurrentTime):
    print(f"Parseando el archivo IPC. Iniciado a las {aCurrentTime} ")

    # Read the XLSM file
    data_df = pd.read_excel(file_path, header=None, skiprows=5, sheet_name=2)

    # transpose the DataFrame
    data_df = data_df.T

    # Drop columns
    columns_to_drop = [1, 2, 3, 17, 18, 22, 23] + list(range(26, 34,1)) + [47, 48, 52, 53] + list(range(56, 64, 1)) + [77, 78, 82, 83] + list(range(86,94,1)) + [107, 108, 112, 113] + list(range(116,124,1)) + [137, 138, 142, 143] + list(range(146,154,1)) + [167, 168, 172, 173] + list(range(176,184,1)) + [197, 198, 202, 203] + list(range(206,len(data_df.columns),1))
    data_df = data_df.drop(columns=data_df.columns[columns_to_drop])
    
    # Drop rows
    data_df = data_df.drop([0])
    


    columnNames = ["date", 
                       "nacionalNivelGeneral", "nacionalAlimBebidasNoAlcohol", "nacionalBebidasAlcoholTabaco", "nacionalPrendasVestirCalzado", "nacionalViviendaAgua", "nacionalEquipamiento", "nacionalSalud", "nacionalTransporte", "nacionalComunicacion", "nacionalRecreacion", "nacionalEducacion", "nacionalRestaurant", "nacionalBsSvsVarios", "nacionalEstacional", "nacionalNucleo", "nacionalRegulados", "nacionalBienes", "nacionalServicios",
                       "gbaNivelGeneral", "gbaAlimBebidasNoAlcohol", "gbaBebidasAlcoholTabaco", "gbaPrendasVestirCalzado", "gbaViviendaAgua", "gbaEquipamiento", "gbaSalud", "gbaTransporte", "gbaComunicacion", "gbaRecreacion", "gbaEducacion", "gbaRestaurant", "gbaBsSvsVarios", "gbaEstacional", "gbaNucleo", "gbaRegulados", "gbaBienes", "gbaServicios",
                       "pampeanaNivelGeneral", "pampeanaAlimBebidasNoAlcohol", "pampeanaBebidasAlcoholTabaco", "pampeanaPrendasVestirCalzado", "pampeanaViviendaAgua", "pampeanaEquipamiento", "pampeanaSalud", "pampeanaTransporte", "pampeanaComunicacion", "pampeanaRecreacion", "pampeanaEducacion", "pampeanaRestaurant", "pampeanaBsSvsVarios", "pampeanaEstacional", "pampeanaNucleo", "pampeanaRegulados", "pampeanaBienes", "pampeanaServicios",
                       "noaNivelGeneral", "noaAlimBebidasNoAlcohol", "noaBebidasAlcoholTabaco", "noaPrendasVestirCalzado", "noaViviendaAgua", "noaEquipamiento", "noaSalud", "noaTransporte", "noaComunicacion", "noaRecreacion", "noaEducacion", "noaRestaurant", "noaBsSvsVarios", "noaEstacional", "noaNucleo", "noaRegulados", "noaBienes", "noaServicios",
                          "neaNivelGeneral", "neaAlimBebidasNoAlcohol", "neaBebidasAlcoholTabaco", "neaPrendasVestirCalzado", "neaViviendaAgua", "neaEquipamiento", "neaSalud", "neaTransporte", "neaComunicacion", "neaRecreacion", "neaEducacion", "neaRestaurant", "neaBsSvsVarios", "neaEstacional", "neaNucleo", "neaRegulados", "neaBienes", "neaServicios",
                            "cuyoNivelGeneral", "cuyoAlimBebidasNoAlcohol", "cuyoBebidasAlcoholTabaco", "cuyoPrendasVestirCalzado", "cuyoViviendaAgua", "cuyoEquipamiento", "cuyoSalud", "cuyoTransporte", "cuyoComunicacion", "cuyoRecreacion", "cuyoEducacion", "cuyoRestaurant", "cuyoBsSvsVarios", "cuyoEstacional", "cuyoNucleo", "cuyoRegulados", "cuyoBienes", "cuyoServicios",
                            "patagoniaNivelGeneral", "patagoniaAlimBebidasNoAlcohol", "patagoniaBebidasAlcoholTabaco", "patagoniaPrendasVestirCalzado", "patagoniaViviendaAgua", "patagoniaEquipamiento", "patagoniaSalud", "patagoniaTransporte", "patagoniaComunicacion", "patagoniaRecreacion", "patagoniaEducacion", "patagoniaRestaurant", "patagoniaBsSvsVarios", "patagoniaEstacional", "patagoniaNucleo", "patagoniaRegulados", "patagoniaBienes", "patagoniaServicios"
    ]

    # Set column names
    data_df.columns = columnNames


    # date column without time
    # INDEC a veces publica la fecha con un día distinto al primero del mes
    # (ej: 2026-03-26 en lugar de 2026-03-01) y no lo corrigen de su lado.
    # El IPC es una serie mensual, así que normalizamos toda fecha al primer
    # día de su mes: arregla ese typo y cualquiera futuro de la misma clase.
    data_df["date"] = (
        pd.to_datetime(data_df["date"]).dt.to_period("M").dt.to_timestamp().dt.date
    )

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

def refreshIPCLargo(db):
    """Refresca el matview public.ipc_largo (IPC empalmado base dic-2016 = 100).

    Corre después del INSERT y en su propia conexión en AUTOCOMMIT porque
    REFRESH MATERIALIZED VIEW CONCURRENTLY no puede ejecutarse dentro de una
    transacción. Si el matview todavía no existe se avisa y se sigue: el ETL
    del IPC no depende de él. Cualquier otro error se propaga a propósito, para
    que la corrida termine con exit code != 0 y el MAILTO del cron avise.
    """
    conn = db.engine.connect().execution_options(isolation_level="AUTOCOMMIT")
    try:
        exists = conn.execute(sqlalchemy.text(
            "SELECT 1 FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = 'public' AND c.relname = 'ipc_largo' AND c.relkind = 'm'"
        )).scalar()

        if not exists:
            print("El matview public.ipc_largo no existe: se omite el refresh.")
            return

        print("Refrescando public.ipc_largo...")
        conn.execute(sqlalchemy.text(
            "REFRESH MATERIALIZED VIEW CONCURRENTLY public.ipc_largo"
        ))
        print("public.ipc_largo refrescado OK.")
    finally:
        conn.close()

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

        # No se usa if_exists='replace': eso emite un DROP TABLE sin CASCADE y
        # Postgres lo rechaza cuando hay objetos dependientes, como la vista
        # public.v_ipc_largo que alimenta el matview public.ipc_largo.
        # TRUNCATE + append es transaccional en Postgres: preserva el esquema de
        # la tabla, no rompe las dependencias y ningún lector ve la tabla vacía.
        tableExists = sqlalchemy.inspect(db.engine).has_table('IPCIndec', schema='public')

        with db.engine.begin() as conn:
            if tableExists:
                conn.execute(sqlalchemy.text('TRUNCATE TABLE public."IPCIndec"'))
            result = df.to_sql(
                name='IPCIndec',
                con=conn,
                if_exists='append' if tableExists else 'replace',
                index=False,
                dtype=dtypeMap,
                schema='public',
            )
        print(f"Number of records inserted as reported by the postgres server: {result}")

        # El IPC empalmado se recalcula recién acá, con los datos ya commiteados.
        refreshIPCLargo(db)

    db.disconnect()

    print(f"IPC grabado OK a las {aCurrentTime} ")

    return True



def main():
    currentTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # bajamos el ipc. Me devuelve (estado, path del archivo)
    estado, file_path = downloadIPC(currentTime)

    # "todavia no publicaron" es el caso NORMAL los primeros dias de la ventana: sale con 0 y
    # no dispara el MAILTO del cron. Un fallo real sale con 1, y ahi si avisa.
    if estado == "no_publicado":
        print("El IPC del mes todavia no esta publicado. Nada que hacer.")
        return 0
    if estado != "ok":
        err("No se pudo descargar el IPC. Se aborta la ejecucion.")
        return 1

    # parseamos el ipc
    currentTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        df = parseIPC(file_path, currentTime)
    finally:
        # el temporal se borra aunque el parseo falle
        os.remove(file_path)

    # grabamos el ipc en la base de datos
    if df is not None:
        currentTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        saveIPC(df, currentTime)

    return 0

if __name__ == "__main__":
    # El exit code es el contrato con el cron: 0 = todo bien o todavia no publicaron,
    # != 0 = algo se rompio y el MAILTO tiene que avisar. Un parseIPC que explote propaga
    # el traceback por stderr y sale != 0 solo, que es exactamente lo que queremos.
    sys.exit(main())
    

