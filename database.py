import psycopg2
from psycopg2 import OperationalError, ProgrammingError


def create_connection(db_name: str, db_user: str, db_password: str,
                      db_host: str, db_port: str):
    """
    Creates connection with postgresql
    :param db_name: "postges"
    :param db_user: username
    :param db_password: password
    :param db_host: host
    :param db_port: port
    :return: connection
    """
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection


def create_database(connection, query: str) -> None:
    """
    Creates database
    :param connection: connection
    :param query: string with query in psql language
    """
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Query executed successfully")
    except ProgrammingError:
        print(f"""It seems that this database is already exists.
              Connect to the existing database""")
    except OperationalError as e:
        print(f"The error '{e}' occurred")


def execute_query(connection, query) -> None:
    """
    Executes queries in psql language
    :param connection: connection
    :param query: string with query in psql language
    """
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Query executed successfully")
    except OperationalError as e:
        print(f"The error '{e}' occurred")


def insert(columns: list, data: list, table_name: str, connection) -> None:
    """
    Inserts data in table
    :param columns: list with names of columns
    :param data: list with tuples, which is inserted in table
    :param table_name: name of the table
    :param connection: connection
    """
    data_records = ", ".join(["%s"] * len(data))

    insert_query = (
        f"""INSERT INTO {table_name} ({', '.join(columns)}) VALUES {data_records}"""
    )
    cursor = connection.cursor()
    try:
        cursor.execute(insert_query, data)
        print("Data inserted successfully")
    except OperationalError as e:
        print(f"The error '{e}' occurred")


def create_table(types: list, table_name: str, connection):
    """
    Creates table by its name and column types and names
    :param types: list with strings, where each string is a
    joined column name with its type
    :param table_name: name of the table
    :param connection: connection
    """
    create_query = \
        f"CREATE TABLE IF NOT EXISTS {table_name} (id SERIAL PRIMARY KEY, {', '.join(types)})"
    return execute_query(connection, create_query)
