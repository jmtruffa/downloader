import requests
import pandas as pd
from dataBaseConn2 import DatabaseConnection
import sqlalchemy
from datetime import datetime
import warnings

def make_request(url):
    
    # Request headers
    headers = {
        "Content-Type": "application/json",
    }

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

    # Save the df to the postgres database updating the existing data
    # connect to the database
    db = DatabaseConnection(db_type="postgresql", db_name= "data")
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
        print(f"Number of records inserted as reported by the postgres server: {result}.")

    db.disconnect()

    return True

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

    # Display the final result DataFrame
    saveToDatabase(final_df)
