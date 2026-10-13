import io
import os
import sys
import time
import requests
from datetime import datetime
import pandas as pd
from dataBaseConn2 import DatabaseConnection
import sqlalchemy


# Fuente: serie larga del IPC por divisiones, en formato largo (una fila por serie, region y
# mes). Reemplaza al sh_ipc_MM_YY.xls desde el 2026-10-13: ese dia INDEC publico el IPC pero
# el .xls del mes no aparecio en el FTP, y este CSV ya tenia el dato nuevo. Ademas el parseo
# deja de depender de la hoja, las filas a saltear y ~60 indices de columnas del Excel.
# Validado contra sh_ipc_09_26.xls: mismas 126 series, diferencia maxima 0.0005 (redondeo).
URL_IPC = "https://www.indec.gob.ar/ftp/cuadros/economia/serie_ipc_divisiones.csv"
ENCABEZADO_CSV = "Codigo;Descripcion;Clasificador;Periodo;Indice_IPC"

# Reintentos de la descarga. Apenas INDEC publica (16:00) el host se satura y corta la
# transferencia a mitad de archivo: el 13/08/2026 trajo 2.170.549 de 2.372.608 bytes.
# Un reintento con espera resuelve ese corte sin esperar a la corrida del dia siguiente.
# El corte llega como excepcion (urllib3 2.x valida el Content-Length por su cuenta), asi
# que no hace falta compararlo a mano. Hacerlo era ademas un bug: con gzip el header trae
# el largo comprimido y response.content el descomprimido.
REINTENTOS = 3
ESPERA_REINTENTO_SEG = 20

# Region del CSV -> prefijo de columna en la tabla IPCIndec.
REGIONES = {
    "Nacional": "nacional", "GBA": "gba", "Pampeana": "pampeana", "Noroeste": "noa",
    "Noreste": "nea", "Cuyo": "cuyo", "Patagonia": "patagonia",
}

# Codigo de serie del CSV -> sufijo de columna en la tabla IPCIndec.
SERIES = {
    "0": "NivelGeneral", "01": "AlimBebidasNoAlcohol", "02": "BebidasAlcoholTabaco",
    "03": "PrendasVestirCalzado", "04": "ViviendaAgua", "05": "Equipamiento", "06": "Salud",
    "07": "Transporte", "08": "Comunicacion", "09": "Recreacion", "10": "Educacion",
    "11": "Restaurant", "12": "BsSvsVarios", "Estacional": "Estacional", "Núcleo": "Nucleo",
    "Regulados": "Regulados", "B": "Bienes", "S": "Servicios",
}

# Orden de columnas de la tabla: date + region x serie, igual que con el .xls.
COLUMNAS = [region + serie for region in REGIONES.values() for serie in SERIES.values()]


def err(mensaje):
    """Escribe en stderr.

    El wrapper manda stdout al log y deja stderr libre a proposito, para que el MAILTO del
    cron avise. Todo lo que sea un fallo real va por aca; el relato normal va por print().
    """
    print(mensaje, file=sys.stderr)


def downloadIPC(aCurrentTime):
    """Baja el CSV del IPC. Devuelve el contenido en bytes, o None si no se pudo."""
    print(URL_IPC)

    for intento in range(1, REINTENTOS + 1):
        try:
            response = requests.get(URL_IPC, timeout=120)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            err(f"Intento {intento}/{REINTENTOS}: fallo la descarga: {e}")
            if intento < REINTENTOS:
                time.sleep(ESPERA_REINTENTO_SEG)
            continue

        contenido = response.content

        # INDEC responde 200 con una pagina HTML cuando un archivo no esta, asi que el status
        # por si solo no alcanza: hay que mirar el contenido.
        if not contenido.decode("latin1", errors="replace").startswith(ENCABEZADO_CSV):
            tipo = response.headers.get("Content-Type", "desconocido")
            err(f"Intento {intento}/{REINTENTOS}: la respuesta no es el CSV esperado "
                f"(Content-Type: {tipo}, {len(contenido)} bytes).")
            if intento < REINTENTOS:
                time.sleep(ESPERA_REINTENTO_SEG)
            continue

        print("------------------------------------")
        print(f"IPC descargado OK a las {aCurrentTime} ({len(contenido)} bytes)")
        return contenido

    return None

def parseIPC(contenido, aCurrentTime):
    print(f"Parseando el archivo IPC. Iniciado a las {aCurrentTime} ")

    data_df = pd.read_csv(io.BytesIO(contenido), sep=";", encoding="latin1", dtype=str,
                          na_values=["NA"], keep_default_na=False)

    # Solo las series que van a la tabla; el CSV podria sumar otras sin romper nada.
    data_df = data_df[data_df["Region"].isin(REGIONES) & data_df["Codigo"].isin(SERIES)].copy()
    data_df["columna"] = data_df["Region"].map(REGIONES) + data_df["Codigo"].map(SERIES)
    data_df["date"] = pd.to_datetime(data_df["Periodo"], format="%Y%m").dt.date
    data_df["valor"] = pd.to_numeric(data_df["Indice_IPC"].str.replace(",", ".", regex=False),
                                     errors="raise")

    if data_df.duplicated(["date", "columna"]).any():
        raise ValueError("El CSV del IPC trae series duplicadas para un mismo mes y region.")

    data_df = data_df.pivot(index="date", columns="columna", values="valor")

    # Si INDEC renombra una region o un codigo, la serie desaparece en silencio. Mejor fallar
    # (y que avise el MAILTO) que pisar la tabla con columnas vacias.
    faltantes = [c for c in COLUMNAS if c not in data_df.columns]
    if faltantes:
        raise ValueError(f"Faltan series en el CSV del IPC: {faltantes}")
    data_df = data_df[COLUMNAS]
    if data_df.isna().any().any():
        raise ValueError("El CSV del IPC trae huecos (NA) en el indice.")

    data_df = data_df.sort_index().reset_index()

    data_dfVar = data_df.copy()
    data_dfVar.columns = ["date"] + ["var" + column[0].upper() + column[1:] for column in data_df.columns[1:]]

    # for every column except date, calculate the percentage change from the previous row
    for column in data_dfVar.columns[1:]:
        data_dfVar[column] = data_dfVar[column].pct_change()

    data_df = pd.concat([data_df.iloc[:,:], data_dfVar.iloc[:, 1:]], axis=1)

    return data_df

def ultimoMesEnBase():
    """Ultimo mes cargado en IPCIndec, o None si la tabla no existe o esta vacia."""
    db = DatabaseConnection(db_type="postgresql", db_name=os.environ.get('POSTGRES_DB'))
    db.connect()
    try:
        if not sqlalchemy.inspect(db.engine).has_table('IPCIndec', schema='public'):
            return None
        with db.engine.connect() as conn:
            return conn.execute(sqlalchemy.text('SELECT max(date) FROM public."IPCIndec"')).scalar()
    finally:
        db.disconnect()

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

    contenido = downloadIPC(currentTime)
    if contenido is None:
        err("No se pudo descargar el IPC. Se aborta la ejecucion.")
        return 1

    currentTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df = parseIPC(contenido, currentTime)

    # El CSV tiene nombre fijo y siempre existe, asi que "todavia no publicaron" se detecta
    # comparando meses: si no trae nada mas nuevo que la base, no hay nada que hacer. Es el
    # caso NORMAL los primeros dias de la ventana y sale con 0, sin disparar el MAILTO.
    ultimoCsv = df["date"].max()
    ultimoBase = ultimoMesEnBase()
    if ultimoBase is not None and ultimoCsv <= ultimoBase:
        print(f"El IPC del mes todavia no esta publicado (ultimo mes: {ultimoCsv}). Nada que hacer.")
        return 0

    print(f"Hay IPC nuevo: {ultimoCsv} (en base: {ultimoBase})")
    currentTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    saveIPC(df, currentTime)

    return 0

if __name__ == "__main__":
    # El exit code es el contrato con el cron: 0 = todo bien o todavia no publicaron,
    # != 0 = algo se rompio y el MAILTO tiene que avisar. Un parseIPC que explote propaga
    # el traceback por stderr y sale != 0 solo, que es exactamente lo que queremos.
    sys.exit(main())
    

