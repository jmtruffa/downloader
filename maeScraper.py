#import selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import os


import pandas as pd

from datetime import datetime

from dataBaseConn2 import DatabaseConnection
import sqlalchemy

currentTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def download():
    """Descarga los montos operados en MAE y devuelve un dataframe con los datos"""


    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--disable-gpu')
    driver = webdriver.Chrome(options = chrome_options)

    urlMAE = "https://www.mae.com.ar/mercados/forex"

    driver.get(urlMAE)


    try:
        # Wait for the table to be present in the DOM
        table_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, 'table-components-container'))
        )

        # Extract the HTML of the table
        table_html = table_element.get_attribute('outerHTML')

        # Use pandas to read the HTML table into a DataFrame
        df = pd.read_html(table_html)[0]

        # add column date with current date as column 0
        df.insert(0, 'date', pd.to_datetime(datetime.now().strftime("%Y-%m-%d")))
        #df.insert(0, 'date', datetime.now().strftime("%Y-%m-%d"))

        # Define a regular expression pattern for extraction from Instrumento column
        #pattern = r'(?P<currencyOut>[A-Z\s]+) \/ (?P<currencyIn>[A-Z]+) (?P<settle>\d+) (?P<settleDate>\d{6})'
        #pattern = r'(?P<currencyOut>[\w$]+)\s*\/\s*(?P<currencyIn>[\w$]+\s*\w*)\s*(?P<settle>\w+)\s*(?P<settleDate>\d{6})'
        #pattern = r'(?P<currencyOut>[\w$]+)\s*\/\s*(?P<currencyIn>[\w$]+(?:\s*\w+)?)\s*(?P<settle>\w+)\s*(?P<settleDate>\d{6})'
        #pattern = r'(?P<currencyOut>[\w$]+)\s*(?:\/\s*)?(?P<currencyIn>[\w$]+(?:\s*\w+)?)\s*(?P<settle>\w+)\s*(?P<settleDate>\d{6})'
        #pattern = r'(?P<currencyOut>[\w$]+(?:\s*\w*)?)\s*(?:\/\s*)?(?P<currencyIn>[\w$]+(?:\s*\w*)?)\s*(?P<settle>\w+)\s*(?P<settleDate>\d{6})'
        #pattern = r'(?P<currencyOut>[\w$]+(?:\s*\w*)?)\s*(?:\/\s*)?(?P<currencyIn>[\w$]+(?:\s*\w*)?)\s*(?P<settle>\w+)\s*(?P<settleDate>\d{5,6})'
        pattern = r'(?P<currencyOut>[\w$]+(?:\s*\w*)?)\s*(?:\/\s*)?(?P<currencyIn>[\w$]+(?:\s*\w*)?)\s*(?P<settle>\w+)\s*(?P<settleDate>\d{6})'
        
        

        # Apply the pattern to the column and extract the information
        df_extracted = df['Instrumento'].str.extract(pattern)

        # Convert settleDate to datetime format
        df_extracted['settleDate'] = pd.to_datetime(df_extracted['settleDate'], format="%d%m%y", errors='coerce')

        # Combine the extracted columns with the original dataframe
        df = pd.concat([df, df_extracted], axis=1)
        
        # Apply a function to add the missing digit in 'settleDate' based on the current year
        #current_year = datetime.now().year % 10
        #df_extracted['settleDate2'] = df_extracted['settleDate'].apply(lambda x: f'{x}{current_year}' if len(x) == 5 else x)

        
        df['settleDate'] = pd.to_datetime(df['settleDate'], format="%d%m%y")

        return df
    

    finally:
        # Close the WebDriver
        driver.quit()

def saveToDatabase(df):
    """Graba los datos en la base de datos postgres"""

    # Save the df to the postgres database updating the existing data
    # connect to the database
    db = DatabaseConnection(db_type="postgresql", db_name= os.environ.get('POSTGRES_DB'))
    db.connect()

    # Check if the table exists
    table_exists_query = f"""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name = 'forex'
    """
    result = pd.read_sql(table_exists_query, db.conn)

    if result.empty:
        data_to_insert = df
    else:
        query = f'SELECT MAX(date) FROM "forex"'
        last_date = pd.read_sql(query, db.conn).iloc[0,0]
        # convert to datetime in order to compare to the date column in data_df
        last_date = pd.to_datetime(last_date)

        # Filter the data to insert
        data_to_insert = df[df['date'] > last_date]


    # Insert the data into the database
    if len(data_to_insert) == 0:
        print("No hay datos nuevos que grabar")
    else:
        print(f"Inserting {len(data_to_insert)} rows into forex Table")
        # use Date type for the 'date' column in the database to get rid of the time part
        dtypeMap = {'date': sqlalchemy.types.Date, 'settleDate': sqlalchemy.types.Date}
        result = data_to_insert.to_sql(name = 'forex', con = db.engine, if_exists = 'append', index = False, dtype=dtypeMap, schema = 'public')
        db.conn.commit()
        print(f"Number of records inserted as reported by the postgres server: {result}.")

    db.disconnect()

    return True


    

if __name__ == "__main__":
    
    max_attempts = 5
    attempts = 0
    forexTable = None
    print('-' * 20)
    while attempts < max_attempts and (forexTable is None or forexTable.empty):

        print(f"Intento {attempts + 1} de scrapear datos de Forex MAE a las {currentTime}")
        forexTable = download()
        attempts += 1

    if forexTable is not None and not forexTable.empty:
        print("Descarga exitosa!")
        saveToDatabase(forexTable)
    else:
        print("El proceso falló 5 veces. Abortando.")

