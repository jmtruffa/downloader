from dataBaseConn import DatabaseConnection
import pandas as pd

def getTable(table):
    
    table = table.upper()
    db = DatabaseConnection("/Users/juan/data/test.sqlite3")
    db.connect()

    query = f"SELECT db FROM tables WHERE name = '{table}'"
    db_result = db.execute_select_query(query)

    db.disconnect()

    if db_result:
        #db_name = db_result[0][0]
        db_name = f"/Users/juan/data/{db_result[0][0]}.sqlite3"
    
        # Connect to the corresponding db
        db = DatabaseConnection(db_name)
        db.connect()
        table_query = f"SELECT * FROM {table}"

        # Retrieve the table as a DataFrame
        table_df = pd.read_sql_query(table_query, db.conn)

        # Disconnect from the corresponding db
        db.disconnect()

        # Convert "date" column to '%Y-%m-%d' format if exists
        if "date" in table_df.columns:
            #table_df["date"] = pd.to_datetime(table_df["date"]).dt.strftime('%Y-%m-%d')
            table_df['date'] = pd.to_datetime(table_df['date'], unit='s').dt.strftime('%Y-%m-%d')

        return table_df

    else:
            print(f"Table '{table}' not found in the 'tables' table.")
            return None