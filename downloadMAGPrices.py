from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import os
from io import StringIO
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

import warnings

import pandas as pd

from datetime import datetime

from dataBaseConn2 import DatabaseConnection
import sqlalchemy

currentTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def download(date_pairs):
    """Baja la info de precios de un día. La fecha debe ser un string en formato DD-MM-YYYY"""

    # Initialize Chrome driver outside the loop
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--disable-gpu')
    driver = webdriver.Chrome(options=chrome_options)

    try:
        # Initialize an empty DataFrame to store the results
        all_data = pd.DataFrame()

        for start_date, end_date in date_pairs:
            
            print(f"Downloading prices for the period: {start_date} to {end_date}")

            # Navigate to the URL
            urlMAG = "https://www.mercadoagroganadero.com.ar/dll/hacienda1.dll/haciinfo000002"
            driver.get(urlMAG)

            # Find the initial date input field and enter the date
            initial_date_input = driver.find_element(By.NAME, "txtFechaIni")
            initial_date_input.clear()
            initial_date_input.send_keys(start_date)

            # Find the ending date input field and enter the date
            ending_date_input = driver.find_element(By.NAME, "txtFechaFin")
            ending_date_input.clear()
            ending_date_input.send_keys(end_date)

            # Find the 'BUSCAR' button and click it
            buscar_button = driver.find_element(By.XPATH, "//button[text()='BUSCAR']")
            buscar_button.click()

            try:
                # Wait for the table to be present in the DOM
                table_element = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, 'table-custom'))
                )

                # Extract the HTML of the table
                table_html = table_element.get_attribute('outerHTML')

                # Use pandas to read the HTML table into a DataFrame
                html_data = StringIO(table_html)
                df = pd.read_html(html_data)[0]

                # agregamos la fecha de los datos que tenemos en start_date
                df['date'] = start_date

                # hacemos que la columna 'date' sea la primera
                cols = df.columns.tolist()
                cols = cols[-1:] + cols[:-1]
                df = df[cols]

                # renombramos las columnas
                df.columns = ['date', 'categoria', 'precioMinimo', 'precioMaximo', 'precioPromedio', 'precioMediana', 'totalCabezas', 'importe', 'kgs', 'promKgs']
                df = df.dropna()




                # Append the current DataFrame to the overall result
                all_data = pd.concat([all_data, df], ignore_index=True)

            

            except Exception as e:
                print(f"An error occurred: {e}")

        return all_data


    finally:
        # Close the WebDriver outside the loop
        driver.quit()

# def generate_date_pairs(start_date, end_date):
#     """Genera pares de fechas para scrapear precios del MAG. Los pares son todos de un día para así obtener la información diaria y no promedio entre fechas consultadas"""
#     date_format = "%d/%m/%Y"
#     start_date_obj = datetime.strptime(start_date, date_format)
#     end_date_obj = datetime.strptime(end_date, date_format)

#     current_date = start_date_obj
#     date_pairs = []

#     while current_date <= end_date_obj:
#         date_pairs.append([current_date.strftime(date_format), current_date.strftime(date_format)])
#         current_date += timedelta(days=1)

#     return date_pairs


def generate_date_pairs(start_date, end_date):
    """Genera pares de fechas para scrapear precios del MAG. Los pares son todos de un día para así obtener la información diaria y no promedio entre fechas consultadas"""
    
    date_format = "%d/%m/%Y"
    current_date = start_date
    date_pairs = []

    while current_date <= end_date:
        date_pairs.append([current_date.strftime(date_format), current_date.strftime(date_format)])
        current_date += timedelta(days=1)

    return date_pairs

def getStartDate(db):
    # get the last date for each ticker in tickers that is not matured
    
    query = f'SELECT MAX(date) as date FROM "MAGPrices"'
    # try to get the data for the ticker. If it fails or returns no data, set the date to datetime(2018, 1, 1)
    try:
        lastDate = db.execute_select_query(query)
    except Exception as e:
        print(f"An error occurred: {e}")
        warnings.warn("Exception. Exiting", UserWarning)
    
    lastDateValue = lastDate.iloc[0,0] if lastDate.iloc[0,0] else 0
    lastDateValue = pd.to_datetime(lastDateValue, format="%Y-%m-%d").date()

    # add 1 day to lastDateValue
    newDate = lastDateValue + timedelta(days=1)
     
            
    return(newDate)

def saveToDatabase(df, db):
    """Graba los datos en la base de datos postgres"""

    df['date'] = pd.to_datetime(df['date'], format="%d/%m/%Y").dt.strftime("%Y-%m-%d")

    # Save the df to the postgres database updating the existing data. If exists, replace it
    dtypeMap = {'date': sqlalchemy.types.Date}
    rowsInserted = df.to_sql(name = 'MAGPrices', con = db.conn, if_exists = 'append', index = False, dtype=dtypeMap, schema = 'public')
    db.conn.commit() # agregado porque decía que grababa pero no lo hacía

    # print number of rows inserted
    print(f"Inserted {rowsInserted} rows")

    # Close the database connection


if __name__ == "__main__":
    print("------------------------------------")
    print(f"MAGPrices. Actualizando precios...{currentTime}")

    # Create a DatabaseConnection instance
    db = DatabaseConnection(db_type="postgresql", db_name= os.environ.get('POSTGRES_DB'))
    db.connect()

    startDate = getStartDate(db)

    #date_pairs = generate_date_pairs(startDate.strftime("%d/%m/%Y"), datetime.now().strftime("%d/%m/%Y"))
    date_pairs = generate_date_pairs(startDate, datetime.now().date())

    # scrapes the data and returns a DataFrame
    result = download(date_pairs)

    saveToDatabase(result, db)

    db.disconnect()
