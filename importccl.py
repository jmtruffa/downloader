import pandas as pd 
from dataBaseConn2 import DatabaseConnection
import sqlalchemy
import os

# write a statement to read the csv file with ";" as separator
df = pd.read_csv('~/Downloads/ccl.csv', sep=';')

# cast column date to datetime
df['date'] = pd.to_datetime(df['date'], format='%d/%m/%y')

df = df.set_index('date')

print(df.info())
print(df)

# Create a DatabaseConnection instance
db = DatabaseConnection(db_type="postgresql", db_name=os.environ.get('POSTGRES_DB'))
db.connect()

#df.to_sql('ccl', db.connection, if_exists='append', index=False, dtype={'date': sqlalchemy.types.Date()})
dtypeMap = {'date': sqlalchemy.types.Date}
res = df.to_sql(name = 'ccl', con = db.conn, if_exists = 'replace', index = True, index_label = 'date', dtype=dtypeMap, schema = 'public')
db.conn.commit()
db.disconnect()
