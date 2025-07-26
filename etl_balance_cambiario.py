import os
import pandas as pd
import sqlalchemy
import requests
import urllib3
import tempfile
from pyxlsb import open_workbook

print("-----------------------------------------------------------------")
print(f"Iniciando descarga de archivos el día {pd.Timestamp.now()}")

temp_filename = None  # inicializar

try:
    # Configuración de conexión a PostgreSQL
    db_user = os.environ.get('POSTGRES_USER')
    db_password = os.environ.get('POSTGRES_PASSWORD')
    db_host = os.environ.get('POSTGRES_HOST')
    db_port = os.environ.get('POSTGRES_PORT', '5432')
    db_name = os.environ.get('POSTGRES_DB')

    engine = sqlalchemy.create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

    # ⚠️ Justificación: el servidor del BCRA tiene problemas de SSL con la cadena de certificados.
    # Se desactiva la verificación porque la fuente es pública, conocida y validada manualmente.
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    url = "https://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/Nuevo-anexo-MC-CLANAE.xlsb"
    response = requests.get(url, verify=False)
    response.raise_for_status()

    if not response.content or len(response.content) < 1000:
        raise ValueError("El archivo descargado está vacío o es sospechosamente pequeño.")
    
    print(f"Archivo descargado correctamente desde {url}")

    # Guardar archivo temporal
    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsb") as tmp_file:
        tmp_file.write(response.content)
        temp_filename = tmp_file.name

    print(f"Archivo temporal creado: {temp_filename}")

    # Leer hoja 1 desde archivo temporal (índice base 1)
    if not os.path.exists(temp_filename):
        raise FileNotFoundError(f"El archivo temporal {temp_filename} no existe.")
    
    # Leer el archivo XLSB
    print(f"Abriendo archivo XLSB: {temp_filename}")
    with open_workbook(temp_filename) as wb:
        with wb.get_sheet(1) as sheet:
            data = []
            for i, row in enumerate(sheet.rows()):
                if i == 0:
                    continue  # Saltar encabezado
                data.append([item.v for item in row])

    if not data:
        raise ValueError("No se pudieron leer datos del archivo Excel.")

    # Convertir a DataFrame y tomar columnas A a I
    df = pd.DataFrame(data).iloc[:, :9]

    # Renombrar columnas
    df.columns = [
        'anexo',
        'mes',
        'sector',
        'concepto_especifico',
        'monto_usd',
        'cuenta',
        'categoria',
        'subcategoria',
        'descripcion_detallada'
    ]

    # Conversión de tipos

    # Manejar fechas correctamente (texto o serial numérico de Excel)
    if pd.api.types.is_numeric_dtype(df['mes']):
        df['mes'] = pd.to_datetime("1899-12-30") + pd.to_timedelta(df['mes'], unit="D")
    else:
        df['mes'] = pd.to_datetime(df['mes'], errors='coerce', dayfirst=True)

    if df['mes'].isna().any():
        raise ValueError("Error: hay fechas no válidas en la columna 'mes' luego del casteo.")

    df['anexo'] = df['anexo'].astype(str).str.strip()
    df['sector'] = df['sector'].astype(str).str.strip()
    df['monto_usd'] = pd.to_numeric(df['monto_usd'], errors='coerce')

    # Reemplazar datos de la tabla
    print("Reemplazando datos en la tabla 'balance_cambiario'...")
    with engine.begin() as connection:
        connection.execute(sqlalchemy.text("TRUNCATE TABLE balance_cambiario;"))
        df.to_sql(
            name='balance_cambiario',
            con=connection,
            if_exists='append',
            index=False,
            dtype={
                'mes': sqlalchemy.types.Date,
                'monto_usd': sqlalchemy.types.Float
            }
        )

    print(f"Descarga de archivos finalizada el día {pd.Timestamp.now()}")

except Exception as e:
    print(f"[ERROR] {type(e).__name__}: {e}")
    exit(1)

finally:
    # Borrar archivo temporal si fue creado
    if temp_filename and os.path.exists(temp_filename):
        os.remove(temp_filename)

print("-----------------------------------------------------------------")
