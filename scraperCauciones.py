import requests
import pandas as pd
from dataBaseConn2 import DatabaseConnection
import sqlalchemy
from datetime import datetime
import warnings
import os

def make_request(url):
    
    # Request headers
    headers = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9,es-US;q=0.8,es;q=0.7',
    'Cache-Control': 'no-cache,no-store,max-age=1,must-revaliidate',
    'Connection': 'keep-alive',
    'Content-Type': 'application/json',
    'Expires': '1',
    'Options': 'renta-fija',
    'Origin': 'https://open.bymadata.com.ar',
    'Pragma': 'no-cache'
}
    #     "Content-Type": "application/json;charset=UTF-8",
    #     #"Content-Type": "application/json",
    #     "token": "dc826d4c2dde7519e882a250359a23a0"
    # }

    # Request payload
    payload = {
        # "T2": True,
        # "T1": True,
        # "T0": True,
        "excludeZeroPxAndQty": True, 
    }

    # Make POST request with SSL verification disabled
    response = requests.post(url, headers=headers, verify=False, json=payload)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse JSON response
        data = response.json()#["data"]

        # Check if response is not empty
        if data:
            # Convert data to Pandas DataFrame
            df = pd.DataFrame(data)
            # add column date with current date as column 0
            df.insert(0, 'date', pd.to_datetime(datetime.now().strftime("%Y-%m-%d")))

            # Display the DataFrame
            #print(df)
            #print(df.info())

            # Return the DataFrame
            return df
        else:
            print(f"Response is empty for URL: {url}")
            return None
    else:
        print(f"Request failed with status code {response.status_code} for URL: {url}")
        print(response.text)
        return None
    
def saveToDatabase(df):
    """Graba los datos en la base de datos postgres"""


    # connect to the database
    db = DatabaseConnection(db_type="postgresql", db_name= os.environ.get('POSTGRES_DB'))
    db.connect()


    # Check if the table exists
    table_exists_query = f"""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name = 'caucionesBYMA'
    """
    result = pd.read_sql(table_exists_query, db.conn)

    if result.empty:
        data_to_insert = df
    else:
        query = f'SELECT MAX(date) FROM "caucionesBYMA"'
        last_date = pd.read_sql(query, db.conn).iloc[0,0]
        # convert to datetime in order to compare to the date column in data_df
        last_date = pd.to_datetime(last_date)

        # Filter the data to insert
        data_to_insert = df[df['date'] > last_date]


    # Insert the data into the database
    if len(data_to_insert) == 0:
        print("No hay datos nuevos que grabar")
    else:
        print(f"Inserting {len(data_to_insert)} rows into cauciones Table")
        # use Date type for the 'date' column in the database to get rid of the time part
        dtypeMap = {'date': sqlalchemy.types.Date}
        result = data_to_insert.to_sql(name = 'caucionesBYMA', con = db.engine, if_exists = 'append', index = False, dtype=dtypeMap, schema = 'public')
        #db.conn.commit()
        print(f"Number of records in caucionesBYMA inserted as reported by the postgres server: {result}.")

        # ahora grabamos la serie capitalizada

        # Construir la serie capitalizada

        # primero traemos la serie de tasas completa
        query = f'SELECT date, vwap, "daysToMaturity" FROM "caucionesBYMA"'
        df = pd.read_sql(query, db.conn)

        # convertimos el campo date a datetime
        df['date'] = pd.to_datetime(df['date'])
        
        # capitalizamos la serie
        serieCapitalizada_df = capitaliza(df)
            
        # grabamos la serie capitalizada en la base de datos
        dtypeMap = {'date': sqlalchemy.types.Date}
        result = serieCapitalizada_df.to_sql('caucionesCapitalizada', db.conn, if_exists='replace', index=False, dtype=dtypeMap, schema = 'public')
        #db.conn.commit()
        print(f"Number of records inserted as reported by the postgres server: {result}.")


    # cerra la conexion a la base de datos
    db.disconnect()

    return True


def capitaliza(df):
    first_date = df['date'].min()
    date_range = pd.date_range(first_date, pd.to_datetime('today')).date

    result_df = pd.DataFrame({'date': date_range})

    markup1 = 0.03
    markup2 = 0.05

    # left_join result_df with df
    
    
    result_df['date'] = pd.to_datetime(result_df['date'])
    result_df = pd.merge(result_df, df, on='date', how='left')
    

    #SELECT date, vwap AS tasa, "daysToMaturity" AS dias
    #FROM public."caucionesBYMA"

    # Calculate the three "tasa efectiva diaria" columns. The first one is the original one, the second one is with a 3% markup and the third one is with a 5% markup
    result_df['ted'] = (1 + result_df['vwap'] / 365 * result_df['daysToMaturity'])**(1/result_df['daysToMaturity']) - 1
    result_df['ted2'] = (1 + (result_df['vwap'] + markup1) / 365 * result_df['daysToMaturity'])**(1/result_df['daysToMaturity']) - 1
    result_df['ted3'] = (1 + (result_df['vwap'] + markup2) / 365 * result_df['daysToMaturity'])**(1/result_df['daysToMaturity']) - 1

    # Fill the NaNs with the previous value
    result_df[['ted', 'ted2', 'ted3']] = result_df[['ted', 'ted2', 'ted3']].ffill()

    # Calculate the factors
    result_df['factor'] = result_df['ted'] + 1
    result_df['factor2'] = result_df['ted2'] + 1
    result_df['factor3'] = result_df['ted3'] + 1

    # Calculate the capitalization series
    result_df['cap'] = result_df['factor'].cumprod()
    result_df['cap1'] = result_df['factor2'].cumprod()
    result_df['cap2'] = result_df['factor3'].cumprod()

    # keep date column, and the last 9 columns
    return result_df.iloc[:, [0] + list(range(-9, 0))]



if __name__ == "__main__":
    """Main function that requests data from the API and saves it to the database. Prints the time of the execution."""

    # Ignore warnings from the requests library regarding SSL certificates
    warnings.filterwarnings("ignore", message="Unverified HTTPS request")
    


    # Print the current time
    print('--' * 20)
    print(f"Iniciando script CAUCIONES a las : {datetime.now()}")
    
    # List of URLs
    urls = [
        "https://open.bymadata.com.ar/vanoms-be-core/rest/api/bymadata/free/cauciones",
    ]

    # Initialize an empty DataFrame to store results
    final_df = pd.DataFrame()

    # Iterate through URLs and append responses to the result DataFrame
    for url in urls:
        print(f"Requesting data from URL: {url}")
        # Make request and get the response DataFrame
        df = make_request(url)
        
        # Append the DataFrame to the result DataFrame
        if df is not None:
            final_df = final_df._append(df, ignore_index=True)

    if final_df is not None:
        saveToDatabase(final_df)
    else:
        print("Exiting via no records to save.")

    # Print the current time
    print(f"Finalizado script CAUCIONES a las : {datetime.now()}")