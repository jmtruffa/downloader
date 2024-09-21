import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError
import os

# Fetch database connection details from environment variables
db_user = os.environ.get('POSTGRES_USER')
db_password = os.environ.get('POSTGRES_PASSWORD')
db_host = os.environ.get('POSTGRES_HOST')
db_port = os.environ.get('POSTGRES_PORT', '5432')  # Default port for PostgreSQL is 5432
db_name = os.environ.get('POSTGRES_DB')

if __name__ == "__main__":
    # Create the database engine
    engine = sqlalchemy.create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
    query = sqlalchemy.text("CALL crea_allrows()")

    try:
        # Connect to the database
        with engine.connect() as connection:
            # Execute the stored procedure with autocommit enabled
            connection.execution_options(isolation_level="AUTOCOMMIT").execute(query)
            print("Stored procedure executed successfully.")

    except SQLAlchemyError as e:
        # Print the error to the console
        print("Error occurred while executing the stored procedure:", str(e))