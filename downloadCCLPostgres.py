import requests
import pandas as pd
from datetime import datetime, timedelta
from dataBaseConn2 import DatabaseConnection
import sqlalchemy
import os




# Function to query the API with specific date range
def query_api(start_date, end_date):
    api_url = "https://apicem.matbarofex.com.ar/api/v2/spot-prices"
    params = {
        "spot": "",
        "from": start_date.strftime("%Y-%m-%d"),
        "to": end_date.strftime("%Y-%m-%d"),
        "page": 1,
        "pageSize": 32000
    }
    response = requests.get(api_url, params=params)
    if response.status_code == 200:
        data = response.json().get('data', [])  # Access the 'data' field
        return pd.DataFrame(data)
    else:
        return None
    
def downloadCCL():
    currentTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("------------------------------------")
    print(f"Actualizando CCL...{currentTime}")

    # Create a DatabaseConnection instance
    #db = DatabaseConnection("/home/juant/data/historicalData.sqlite3")
    db = DatabaseConnection(db_type='postgresql', db_name=os.environ.get('POSTGRES_DB'))
    db.connect()

    # Query the last date in the table
    last_date_query = "SELECT MAX(date) AS max_date FROM ccl"
    last_date_result = db.execute_select_query(last_date_query)
    
    last_date = last_date_result.iloc[0,0] if last_date_result.iloc[0,0] else datetime(2000, 1, 1)
    last_date = pd.to_datetime(last_date, format="%Y-%m-%d").date()
        
    # Calculate the end date (today) and handle holidays
    end_date = datetime.now().date()

    # Convert the Excel date to a Python datetime object
    # ya es una fecha. no necesito ajustarla desde 1970
    #newDate = (datetime(1970, 1, 1) + timedelta(days=last_date).date())

    # Add one day to the date
    newDate = last_date + timedelta(days=1)

    # Query the API for data
    df = query_api(newDate, end_date)

    if not df.empty:

        # Check if there is a row with the value 'CCL' in the 'spot' column
        if not df['spot'].str.contains('CCL').any():
            print("No hay datos para insertar ya que no hay un 'spot' con 'CCL'")
            return
                
        # Filter rows with 'spot' starting with 'CCL'
        df = df[df['spot'].str.startswith('CCL')]

        # Drop unnecessary columns
        df = df.drop(columns=['representativity', 'resolution', 'price'])

        # Pivot to wider format
        df = df.pivot(index='dateTime', columns='spot', values='normalizedPrice')

        # Create the 'ccl' column
        df['ccl'] = df['CCL'].combine_first(df['CCL3']).combine_first(df['CCL2']).combine_first(df['CCL1'])
        df['ccl3'] = df['CCL3'].astype(float)
        df = df.drop(columns=['CCL', 'CCL3', 'CCL2', 'CCL1'])
        

        # Convert 'ccl' to double
        df['ccl'] = df['ccl'].astype(float)
        

        # Rename 'dateTime' to 'date' and change its format
        df['date'] = pd.to_datetime(df.index)
        #df["date"] = (df["date"] - pd.Timestamp("1970-01-01")) // pd.Timedelta("1D")
        
        #df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d").view('int64') // 10**9
        df = df.set_index('date')

        # Append the DataFrame to the database table
        #df.to_sql(name = 'cclTemp', con = db.conn, if_exists = 'append', index = True, index_label = 'date')
        dtypeMap = {'date': sqlalchemy.types.Date}
        rowsInserted = df.to_sql(name = 'ccl', con = db.conn, if_exists = 'append', index = True, index_label = 'date', dtype=dtypeMap, schema = 'public')
        db.conn.commit() # agregado porque decía que grababa pero no lo hacía

        # print number of rows inserted
        print(f"Inserted {rowsInserted} rows")

    else:
        print("No data to insert")

    # Disconnect from the database
    db.disconnect()


if __name__ == "__main__":
    
    downloadCCL()
    
