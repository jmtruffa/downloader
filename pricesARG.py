# Description: This script is used to get historical market data from PPI API
from ppi_client.api.constants import ACCOUNTDATA_TYPE_ACCOUNT_NOTIFICATION, ACCOUNTDATA_TYPE_PUSH_NOTIFICATION, \
    ACCOUNTDATA_TYPE_ORDER_NOTIFICATION
from ppi_client.ppi import PPI
from datetime import datetime, timedelta
import os
import pandas as pd
from dataBaseConn2 import DatabaseConnection
import sqlalchemy
currentTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def login():

# Change login credential to connect to the API
    ppi = PPI(sandbox=False)
    ppi.account.login_api(os.getenv('PPI_API_KEY'), os.getenv('PPI_SECRET_KEY'))
    return ppi

def gettickers(db):
     
    query = 'SELECT ticker, type FROM "sets" WHERE nombre=\'pricesArg\''
    tickers = pd.read_sql(query, db.conn)
    return(tickers)

def getLastDates(db, tickers):
    # get the last date for each ticker in tickers that is not matured
    ticks = tickers['ticker'].tolist()
    query = f'SELECT ticker, MAX(date) as date FROM "pricesARG" WHERE ticker IN {tuple(ticks)} AND matured = \'false\' GROUP BY ticker'
    # try to get the data for the ticker. If it fails or returns no data, set the date to datetime(2018, 1, 1)
    try:
        lastDates = pd.read_sql(query, db.conn)
    except Exception as e:
        print(f"An error occurred: {e}")
        lastDates = pd.DataFrame(columns=['ticker', 'date'])
    else:
        if lastDates.empty:
            lastDates = pd.DataFrame(columns=['ticker', 'date'])
        else:
            # Convert the date column to datetime and add 1 day
            lastDates['date'] = pd.to_datetime(lastDates['date'], format='%Y-%m-%d') + timedelta(days=1)
            
    return(lastDates)

def get_historical_market_data(ppi, tickers, date2 = datetime.now().date()):
    # Search Historic MarketData for the tickers in tickers
    
    # create empty dataframe with ticker, date, price, volume, openingPrice, max, min fields
    result_df = pd.DataFrame(columns=['ticker', 'date', 'price', 'volume', 'openingPrice', 'max', 'min'])
    fail = []
    print('Agregando tickers...')
    for index, row in tickers.iterrows():
        # try to get the data for the ticker
        try:
            result = pd.DataFrame(ppi.marketdata.search(row['ticker'], row['type'], "A-48HS", row['date'], datetime.now().date()))
        except Exception as e:
            # append the ticker to the fail list
            print(f"falló el ticker: {row['ticker']}")
            fail.append(row['ticker'])
        else:
            # if no error, append the data to the dataframe
            if not result.empty:
                result['ticker'] = row['ticker']
                result['matured'] = False
                result_df = pd.concat([result_df, result], ignore_index=True)

    return(result_df, fail)

def saveToDatabase(result_df, db):
    """Graba los datos en la base de datos postgres"""

    # Create a DatabaseConnection instance

    # Save the df to the postgres database updating the existing data. 
    dtypeMap = {'date': sqlalchemy.types.Date}
    rowsInserted = result_df.to_sql(name = 'pricesARG', con = db.conn, if_exists = 'append', index = False, dtype=dtypeMap, schema = 'public')
    db.conn.commit() # agregado porque decía que grababa pero no lo hacía
    db.disconnect()

    print(f"Inserted {rowsInserted} rows")

    return(rowsInserted)

def main():
    print("------------------------------------")
    print(f"PricesARG. Actualizando precios...{currentTime}")
    ppi=login()

    db = DatabaseConnection(db_type="postgresql", db_name=os.environ.get('POSTGRES_DB'))
    db.connect()

    tickers = gettickers(db)

    lastDates = getLastDates(db, tickers)

    # left join tickers and lastDates by ticker
    tickers = pd.merge(tickers, lastDates, on='ticker', how='left')

    # filter tickers to get only the ones that have a date
    tickers = tickers[tickers['date'].notna()]


    result_df, fail = get_historical_market_data(ppi, tickers) 

    # query to delete all the records that has the same date as today
    query = f'DELETE FROM "pricesARG" WHERE date = \'{datetime.now().date()}\''
    db.execute_query(query)
    
    # save the data to the database
    saveToDatabase(result_df, db)


if __name__ == "__main__":
    main()