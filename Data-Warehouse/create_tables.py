"""
This module contains functions designed for dropping and creating tables in a PostgreSQL database. 
It retrieves database configuration details from 'dwh.cfg', 
establishes a connection to the database, checks for existing tables, and subsequently drops them. Following this, it creates new tables based on the provided SQL queries.
"""

import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Executes the dropping of each table by utilizing the queries specified in the drop_table_queries list.
    Parameters:
    - cur: Cursor representing the database connection for executing PostgreSQL commands..
    - conn: Database connection responsible for committing the changes.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Generates tables based on the queries provided in the create_table_queries list.
	Each query is displayed before execution. Once all tables are successfully created, a confirmation message is printed.
    Parameters:
    - cur: Cursor representing the database connection for executing PostgreSQL commands.
    - conn: Database connection responsible for committing the changes.
    """
    for query in create_table_queries:
        print("Query: {}".format(query))
        cur.execute(query)
        conn.commit()
    print ("Create table success!")


def main():
    """
    Primary function orchestrating the workflow for connecting to the PostgreSQL database, 
	handling the process of dropping existing tables, creating new tables, and subsequently closing the connection. 
	Database configurations are retrieved from the 'dwh.cfg' file.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print("Connection to aws success!")
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()