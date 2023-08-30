from dataBaseConn import DatabaseConnection
import pandas as pd

def getTable(table, freq = None):
    """Devuelve la tabla especificada en el argumento 'table' como un DataFrame. La obtiene de la base de datos 'test.sqlite3'"""
    
    # Obtenemos el nombre de la base de datos a la que pertenece la tabla
    #table = table.upper()

    db = DatabaseConnection("/Users/juan/data/test.sqlite3")
    db.connect()
    query = f"SELECT db FROM tables WHERE name = '{table}'"
    db_result = db.execute_select_query(query)
    db.disconnect()

    # Si la tabla existe en la base de datos
    if db_result:
        # Construimos el path de la base de datos
        db_name = f"/Users/juan/data/{db_result[0][0]}.sqlite3"
    
        # Nos conectamos a la base de datos
        db = DatabaseConnection(db_name)
        db.connect()

        # Construimos la query para obtener la tabla
        table_query = f"SELECT * FROM {table}"

        # Si se especifica una frecuencia, la añadimos a la query
        if freq:
            table_query += f" WHERE tipoSerie = '{freq}'"

        # Obtenemos la tabla como un DataFrame
        table_df = pd.read_sql_query(table_query, db.conn)

        # Cerramos la conexión
        db.disconnect()

        # Si la tabla tiene una columna 'date', la convertimos a formato fecha
        if "date" in table_df.columns:
            #table_df["date"] = pd.to_datetime(table_df["date"]).dt.strftime('%Y-%m-%d')
            table_df['date'] = pd.to_datetime(table_df['date'], unit='s').dt.strftime('%Y-%m-%d')

        return table_df

    # Si la tabla no existe en la base de datos
    else:
            print(f"Table '{table}' not found in the 'tables' table.")
            return None