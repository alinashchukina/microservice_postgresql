import numpy as np
import psycopg2
from psycopg2.extensions import register_adapter
import click
from preprocess import read_file, preprocess
from database import create_connection, create_database, execute_query
from database import insert, create_table


@click.group()
def cli():
    pass


@cli.command()
@click.option("--n", default=0, type=int)
@click.option("--port", default="5432", nargs=1)
@click.option("--host", default="127.0.0.1", nargs=1)
@click.option("--password", default="12345", nargs=1)
@click.option("--user", default="postgres", nargs=1)
@click.option("--database", default="mydatabase", nargs=1)
@click.argument("path", nargs=1)
def start(path: str, database: str = "mydatabase", user: str = "postgres",
          password: str = "12345", host: str = "127.0.0.1", port: str = "5432",
          n: int = 0) -> None:
    """
    Gets the name of the file with path to it and optional parameters
    The body of service
    Creates psql connection and database
    Then reads .csv or .xlsx file, gets column names and types from it
    Then adds data if the table with such name already exists
    Creates the table and adds the data inside if the table with such name
    doesn't exist
    :param path: the name of the file with path to it
    :param database: name of the database
    :param user: name of psql user
    :param password: password of psql user
    :param host: host
    :param port: port
    :param n: number of row with headers
    """
    register_adapter(np.int64, psycopg2._psycopg.AsIs)

    connection = create_connection("postgres", user, password, host, port)
    create_database_query = "CREATE DATABASE " + database
    create_database(connection, create_database_query)
    connection = create_connection(database, user, password, host, port)

    table, table_name = read_file(path, n)

    cursor = connection.cursor()
    cursor.execute("select * from information_schema.tables where table_name=%s",
                   (table_name,))
    columns, data, types = preprocess(table)

    if bool(cursor.rowcount):
        insert(columns, data, table_name, connection)
        connection.commit()
    else:
        create_table(types, table_name, connection)
        insert(columns, data, table_name, connection)
        connection.commit()


if __name__ == "__main__":
    cli()
