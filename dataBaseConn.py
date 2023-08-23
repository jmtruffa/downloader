import sqlite3

class DatabaseConnection:
    def __init__(self, db_name):
        self.db_name = db_name
        self.conn = None
        self.cursor = None

    def connect(self):
        self.conn = sqlite3.connect(self.db_name)

    def disconnect(self):
        if self.conn:
            self.conn.close()

    def execute_query(self, query):
        if self.conn:
            cursor = self.conn.cursor()
            cursor.execute(query)
            self.conn.commit()

    def execute_select_query(self, query):
        if self.conn:
            cursor = self.conn.cursor()
            cursor.execute(query)
            return cursor.fetchall()

    def create_table(self, table_name, columns):
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
        self.execute_query(query)

    def insert_data(self, table_name, data):
        columns = ", ".join(data.keys())
        values = ", ".join([f"'{value}'" if isinstance(value, str) else str(value) for value in data.values()])
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
        self.execute_query(query)

    def insert_data_many(self, table_name, data_list):
        columns = data_list[0].keys()
        values = [tuple(data.values()) for data in data_list]
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['?'] * len(columns))})"
        self.conn.executemany(query, values)
        self.conn.commit()