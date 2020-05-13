# create_tables.py
#
# PROGRAMMER: Brian Pederson
# DATE CREATED: 02/18/2020
# PURPOSE: Script to create staging and DWH tables for DWH Project 2.
#
# Included functions:
#     drop_tables         - drop staging and DWH tables
#     create_tables       - create staging and DWH tables
#     main                - main function performs table creation
#

import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        print(query[:query.find("(")].strip())
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()