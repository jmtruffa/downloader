"""Descarga el CPI-U de EEUU (BLS) y lo graba en public."USCPI".

Serie CUUR0000SA0: CPI-U, US city average, all items, NOT seasonally adjusted.
Es el deflactor de las series en dólares (ver uscpiMensual.sql y deflactores).

SIN SELENIUM. La versión anterior levantaba Chrome headless con xvfb-run para
bajar un archivo de texto separado por tabs. El motivo real del bloqueo no era la
falta de un browser: BLS responde 403 tanto sin User-Agent como con un UA de
browser, y devuelve 200 cuando el UA identifica al cliente con un contacto, que es
lo que pide su política de acceso. Verificado el 2026-08-04:

    sin headers                       -> 403
    UA de Chrome                      -> 403
    'outlier-etl/1.0 (mail)'          -> 200, 2,7 MB de TSV limpio

Así que alcanza requests y este ETL ya no necesita Chrome ni xvfb en la máquina.

FORMATO DEL ARCHIVO: TSV con columnas series_id, year, period, value,
footnote_codes. `period` es M01..M12 y además M13 = PROMEDIO ANUAL, que se guarda
igual (la vista uscpi_mensual lo descarta). Las columnas vienen con relleno de
espacios, así que hay que hacer strip.

VALORES FALTANTES: BLS publica la fila con value = '-' y footnote_codes = 'X'
cuando el dato no está disponible. Hoy pasa con octubre-2025, que no se publicó.
Esas filas se guardan con value NULL en lugar de romper la carga: la vista
uscpi_mensual las rellena por interpolación geométrica y las marca. Si algún día
BLS publica ese mes, el valor real entra solo y la interpolación se apaga.
"""
import os
from io import StringIO

import pandas as pd
import requests
import sqlalchemy
from sqlalchemy import create_engine, text

URL = "https://download.bls.gov/pub/time.series/cu/cu.data.1.AllItems"
SERIES_ID = "CUUR0000SA0"
TABLE = "USCPI"
# BLS exige que el User-Agent identifique al cliente con un contacto.
USER_AGENT = "outlier-etl/1.0 (data@outlier.com.ar)"
# Guarda de cordura: la serie arranca en 1913, hoy son ~1475 filas. Si vinieran
# muchas menos, algo salió mal en la descarga y no queremos pisar la tabla buena.
MIN_FILAS = 1400

db_user = os.environ.get('POSTGRES_USER')
db_password = os.environ.get('POSTGRES_PASSWORD')
db_host = os.environ.get('POSTGRES_HOST')
db_port = os.environ.get('POSTGRES_PORT', '5432')
db_name = os.environ.get('POSTGRES_DB')

dtypeMap = {'year': sqlalchemy.types.Integer, 'value': sqlalchemy.types.Float}


def downloadUSCPI():
    """Baja el archivo del BLS y devuelve el DataFrame de la serie, o None."""
    try:
        resp = requests.get(URL, headers={"User-Agent": USER_AGENT}, timeout=120)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"No se pudo descargar el archivo del BLS: {e}")
        return None

    df = pd.read_csv(StringIO(resp.text), sep='\t')

    # Los encabezados y los valores de texto vienen con relleno de espacios.
    df.columns = [c.strip() for c in df.columns]
    for col in df.select_dtypes('object'):
        df[col] = df[col].str.strip()

    df = df[df['series_id'] == SERIES_ID].copy()

    # `value` llega como texto porque BLS usa '-' para los meses sin dato.
    # errors='coerce' los convierte en NaN, que pandas graba como NULL, en lugar
    # de reventar el insert con "invalid input syntax for type double precision".
    sinDato = pd.to_numeric(df['value'], errors='coerce').isna() & df['value'].notna()
    df['value'] = pd.to_numeric(df['value'], errors='coerce')

    if len(df) < MIN_FILAS:
        print(f"Solo {len(df)} filas para {SERIES_ID} (esperaba al menos {MIN_FILAS}): "
              "la descarga parece incompleta. No se toca la tabla.")
        return None

    mensuales = df[df['period'] != 'M13']
    ultimo = mensuales.sort_values(['year', 'period']).iloc[-1]
    print(f"{len(df)} filas para {SERIES_ID}. "
          f"Último mes: {ultimo['year']}-{ultimo['period']} = {ultimo['value']}")
    if sinDato.any():
        detalle = ", ".join(
            f"{r.year}-{r.period}" for r in df[sinDato].itertuples())
        print(f"Meses publicados sin valor (quedan NULL, los rellena "
              f"uscpi_mensual): {detalle}")

    return df


def saveUSCPI(df):
    """Reemplaza el contenido de la tabla sin destruirla.

    No se usa to_sql(if_exists='replace'): eso emite un DROP TABLE sin CASCADE y
    Postgres lo rechaza porque de esta tabla cuelga la vista uscpi_mensual, que a
    su vez alimenta deflactores y la matview prestamos_pm_real. TRUNCATE + append
    es transaccional: preserva el esquema, no rompe dependencias y ningún lector
    ve la tabla vacía.

    Gemelo de replaceTable() en serieseDownloaderPostgres.py. Si aparece un tercer
    ETL con la misma necesidad, conviene extraerlo a un módulo compartido.
    """
    engine = create_engine(
        f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
    try:
        inspector = sqlalchemy.inspect(engine)
        tableExists = inspector.has_table(TABLE, schema='public')

        if tableExists:
            enTabla = {c['name'] for c in inspector.get_columns(TABLE, schema='public')}
            if enTabla != set(df.columns):
                raise RuntimeError(
                    f'public."{TABLE}": las columnas no coinciden con las de la tabla. '
                    f'Sobran en el origen: {sorted(set(df.columns) - enTabla)}. '
                    f'Faltan en el origen: {sorted(enTabla - set(df.columns))}. '
                    'TRUNCATE + append no cambia el esquema: migrar la tabla y '
                    'volver a correr.'
                )

        print(f"Grabando {len(df)} filas en public.\"{TABLE}\"...")
        with engine.begin() as con:
            if tableExists:
                con.execute(text(f'TRUNCATE TABLE public."{TABLE}"'))
            df.to_sql(
                name=TABLE,
                con=con,
                if_exists='append' if tableExists else 'replace',
                index=False,
                schema='public',
                dtype=dtypeMap,
            )
        print("USCPI grabado OK.")
    finally:
        engine.dispose()

    return True


def main():
    df = downloadUSCPI()
    if df is None:
        print("El proceso no obtuvo datos válidos. Abortando sin tocar la tabla.")
        return False

    saveUSCPI(df)
    return True


if __name__ == "__main__":
    import sys
    sys.exit(0 if main() else 1)
