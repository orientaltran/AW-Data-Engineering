"""
This module encompasses functions designed for loading data into staging tables and inserting data into analytics tables.
It retrieves SQL queries from the 'sql_queries.py' file, establishes a connection to the database,
executes the queries, and commits the resulting changes.
"""

import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Imports data into staging tables.
    """
    for query in copy_table_queries:
        print("Query loading: {}".format(query))
        cur.execute(query)
        conn.commit()

# Insert data tables
def insert_tables(cur, conn):
    """
    Insert data into analytics tables.
    """
    for query in insert_table_queries:
        print("Query insert: {}".format(query))
        cur.execute(query)
        conn.commit()


def main():
    """
	Main.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    print("Connection to aws success!")
    conn.close()


if __name__ == "__main__":
    main()