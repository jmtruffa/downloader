import os
import tempfile
import pandas as pd
import requests
from urllib3.exceptions import InsecureRequestWarning
import datetime

import sys
import sqlalchemy


def download():
    """
    Descarga el archivo de pases del BCRA
    """

    url = "https://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/Datapases.xlsx"

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
            print(f"Archivo de pases descargado exitosamente a las {current_time}")            
    except requests.exceptions.RequestException as e:
        print(f"Error descargando el archivo. Hora:{current_time}. Error: {e}")
        return False

    return file_path


def parsePases(file_path = None):
    """
    Parsea el archivo de pases del BCRA que viene en format XLSX
    """
    pass


def graba(df):
    """
    Graba el dataframe en la base de datos
    """
    pass


if __name__ == "__main__":
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Descargando archivo de pases a las {current_time}")
    file_path = download()
    if file_path:
        df = parsePases(file_path)
        if df is not None:
            current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"Grabando en la base de datos a las {current_time}")
            graba(df)
            current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"Proceso finalizado a las {current_time}. Borrando archivo descargado.")
            os.remove(file_path)
    else:
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print("Error descargando el archivo. Saliendo a las {current_time}")
        sys.exit(1)