import selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import pandas as pd

from dataBaseConn import DatabaseConnection
from tableFuns import getTable

from datetime import datetime, timedelta



def downloadBadlar(fechaDesde = "01/01/2003"):

    driver = webdriver.Chrome()

    urlBadlar = "https://bcra.gob.ar/PublicacionesEstadisticas/Principales_variables_datos.asp?serie=7937&detalle=BADLAR%20en%20pesos%20de%20bancos%20privados%20(en%20%%20e.a.)"

    driver.get(urlBadlar)
    
    xPathDesde = "/html/body/div[1]/div[2]/div/div[2]/div/div/form/input[1]"
    xPathHasta = "/html/body/div[1]/div[2]/div/div[2]/div/div/form/input[2]"
    xPathSubmit = "/html/body/div/div[2]/div/div[2]/div/div/form/button"
    fechaHasta = (datetime.now() - timedelta(days=1)).strftime("%d%m%Y")

    driver.find_element(By.XPATH, xPathDesde).send_keys("01/01/2023")
    driver.find_element(By.XPATH, xPathHasta).send_keys(fechaHasta)
    driver.find_element(By.XPATH, xPathSubmit).click()

    wait = WebDriverWait(driver, 5)

    # Identify the table with headers "Fecha" and "Valor"
    table = wait.until(EC.presence_of_element_located((By.XPATH, "//table[contains(., 'Fecha') and contains(., 'Valor')]")))

    # Parse the table into a Pandas DataFrame
    tables = pd.read_html(table.get_attribute('outerHTML'))

    # Find the table with headers "Fecha" and "Valor"
    for df in tables:
        if "Fecha" in df.columns and "Valor" in df.columns:
            break

    # Now df contains the desired table
    # Check if the 
    

def calculateFrom():
    """Chequea la última fecha disponible de Badlar en la base de datos. Tabla "tasas" """
    db = DatabaseConnection("/Users/juan/data/dataBCRA.sqlite3")
    db.connect()
    lastDateQuery = "SELECT MAX(date) FROM tasas"
    lastDateResult = db.execute_select_query(lastDateQuery)
    db.disconnect()
    lastDate = lastDateResult[0][0] if lastDateResult[0][0] else 0
    
    

    return lastDate

if __name__ == "__main__":
    # Busca la última fecha de Badlar en la base de datos
    lastDateBadlar = lastDateBadlar()
    Hay que ajustarla ya que está en timestamp y convertirla a datetime
    
    lastDateBadlar = datetime.fromtimestamp(lastDateBadlar) - timedelta(hours=-3)

    downloadBadlar(fechaDesde=lastDateBadlar)







