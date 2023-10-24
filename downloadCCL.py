import requests
import pandas as pd
from datetime import datetime, timedelta
from dataBaseConn import DatabaseConnection

# Define the database file path
db_file = "/Users/juan/data/historicalData.sqlite3"


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

# Create a DatabaseConnection instance
db = DatabaseConnection(db_file)

# Connect to the database
db.connect()

# Query the last date in the table
last_date_query = "SELECT MAX(date) FROM ccl"
last_date_result = db.execute_select_query(last_date_query)
last_date = last_date_result[0][0] if last_date_result[0][0] else datetime(2000, 1, 1)

# Calculate the end date (today) and handle holidays
end_date = datetime.now()

# Convert the Excel date to a Python datetime object
newDate = (datetime(1970, 1, 1) + timedelta(days=last_date)).date()

# Add one day to the date
newDate += timedelta(days=1)

# Query the API for data
df = query_api(newDate + timedelta(days=1), end_date)

if not df.empty:
            
    # Filter rows with 'spot' starting with 'CCL'
    df = df[df['spot'].str.startswith('CCL')]

    # Drop unnecessary columns
    df = df.drop(columns=['representativity', 'resolution', 'price'])

    # Pivot to wider format
    df = df.pivot(index='dateTime', columns='spot', values='normalizedPrice')

    # Create the 'ccl' column
    df['ccl'] = df['CCL'].combine_first(df['CCL3']).combine_first(df['CCL2']).combine_first(df['CCL1'])
    df = df.drop(columns=['CCL'])

    # Convert 'ccl' to double
    df['ccl'] = df['ccl'].astype(float)

    # Rename 'dateTime' to 'date' and change its format
    df['date'] = pd.to_datetime(df.index)
    df["date"] = (df["date"] - pd.Timestamp("1970-01-01")) // pd.Timedelta("1D")
    
    #df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d").view('int64') // 10**9
    df = df.set_index('date')

    df = df.drop(columns=['CCL2', 'CCL1'])

    # Append the DataFrame to the database table
    db.insert_data_many('ccl', df.reset_index, Append=True)

    # print number of rows inserted
    print(f"Inserted {len(df)} rows")

else:
    print("No data to insert")

# Disconnect from the database
db.disconnect()
