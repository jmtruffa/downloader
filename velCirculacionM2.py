import pandas as pd 
from statsmodels.tsa.x13 import x13_arima_analysis
from dataBaseConn2 import DatabaseConnection
import os


def main():

    # get series from database using dataconnection2 
    db = DatabaseConnection(db_type="postgresql", db_name=os.environ.get('POSTGRES_DB'))
    db.connect()

    # get series from database
    queryM2 = 'SELECT date, "M2" FROM "agregadosPrivados"'
    M2 = pd.read_sql(queryM2, db.conn)

    queryIPC = 'SELECT date, "nacionalNivelGeneral" FROM "IPCIndec"'
    IPC = pd.read_sql(queryIPC, db.conn)

    df = pd.merge(M2, IPC, on='date')
    df['M2Real'] = df['M2'] / df['nacionalNivelGeneral']



    #apply x13 to M2Real
    x13 = x13_arima_analysis(df['M2Real'], x12path='/home/juant/dev/bin/x13as')
    x13Results = x13.seasadj
    x13Results.plot()
    
    print(x13Results.info())
    print(df)


if __name__ == "__main__":
    main()