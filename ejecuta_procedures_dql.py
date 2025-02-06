import os
import sys
from sqlalchemy import create_engine, text
from datetime import datetime

# Load environment variables
db_user = os.environ.get('POSTGRES_USER')
db_password = os.environ.get('POSTGRES_PASSWORD')
db_host = os.environ.get('POSTGRES_HOST')
db_port = os.environ.get('POSTGRES_PORT', '5432')  # Default PostgreSQL port
db_name = os.environ.get('POSTGRES_DB')

# Create the connection string
DATABASE_URL = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

def read_procedures_from_file(filename):
    """Reads procedure names from a file, ignoring empty lines and comments."""
    procedures = []
    try:
        with open(filename, "r") as file:
            for line in file:
                proc_name = line.strip()
                if proc_name and not proc_name.startswith("#"):
                    procedures.append(proc_name)
    except FileNotFoundError:
        print(f"Error: El archivo '{filename}' no fue encontrado.")
        sys.exit(1)
    except Exception as e:
        print(f"Error leyendo el archivo de procedimientos: {e}")
        sys.exit(1)

    return procedures

def execute_procedure(engine, procedure_name):
    """Executes a PostgreSQL stored procedure with autocommit enabled."""
    with engine.connect() as connection:
        try:
            print(f"Iniciando ejecución de procedimiento {procedure_name} a las {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")  # ✅ Log the start of the procedure execution
            connection.execution_options(isolation_level="AUTOCOMMIT")  # ✅ Disable SQLAlchemy transaction management. The procedures already contain a COMMIT.
            connection.execute(text(f"CALL {procedure_name}();"))
            print(f"El procedimiento {procedure_name} se ejecutó exitosamente a las  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.")
        except Exception as e:
            print(f"Error executing procedure {procedure_name}: {e}")


def main():
    """Main function to execute stored procedures from a given file."""
    # Check for correct argument usage
    if len(sys.argv) != 2:
        print(f"Uso: {sys.argv[0]} /path/to/procedures_file")
        sys.exit(1)

    # Get the procedure file path from command-line arguments
    procedure_file = sys.argv[1]

    # Initialize database engine
    engine = create_engine(DATABASE_URL)

    # Read procedures from the provided file
    procedures = read_procedures_from_file(procedure_file)

    if not procedures:
        print("No hay procedimientos especificados en el archivo. Saliendo.")
        sys.exit(1)

    # Execute each procedure
    for proc in procedures:
        execute_procedure(engine, proc)

if __name__ == "__main__":
    main()
