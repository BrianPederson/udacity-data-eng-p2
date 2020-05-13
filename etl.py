# etl.py
#
# PROGRAMMER: Brian Pederson
# DATE CREATED: 02/20/2020
# PURPOSE: Script to simulate simple ETL processes for staging and DWH tables for DWH Project 2.
#
# Included functions:
#     get_input_args      - Retrieves and parses the three optional arguments provided by the user
#     load_staging_tables - load staging tables
#     insert_tables       - load DWH tables via staging tables
#     count_tables        - for all tables count the number of rows
#     main                - main function performs ETL load
#

import configparser
import psycopg2
import argparse
import time
from sql_queries import copy_table_queries, insert_table_queries, count_table_queries


# Argument parser utility specific to train.py
def get_input_args():
    """
    Retrieves and parses the three optional arguments provided by the user.

    Mandatory command line arguments:
      None
    Optional command line arguments:
      1. Flag for "skip load staging" as --skip_staging with default False
      2. Flag for "skip load dwh" as --skip_dwh with default False
      3. Flag for "skip count tables" as --skip_count

    This function returns these arguments as an ArgumentParser object.
    Parameters:
      None - using argparse module to create & store command line arguments
    Returns:
      parser.namespace - data structure that stores the command line arguments object
    """
    # Create Parse using ArgumentParser
    parser = argparse.ArgumentParser()
    parser.prog = 'etl.py'
    parser.description = "Performs ETL functions to load staging and DWH tables."

    # Argument 1:
    parser.add_argument('--skip_staging', '-xs', action="store_true",
                        help = "Flag to skip loading of staging files")
    # Argument 2:
    parser.add_argument('--skip_dwh', '-xd', action="store_true",
                        help = "Flag to skip loading of DWH files")
    # Argument 3:
    parser.add_argument('--skip_count', '-xc', action="store_true",
                        help = "Flag to skip counting of staging and DWH files")

    # Note: this will perform system exit if argument is malformed or imcomplete
    in_args = parser.parse_args()

    # return parsed argument collection
    return in_args 


def load_staging_tables(cur, conn):
    """
    load_staging_tables - for all staging tables execute a COPY statement to load staging table from S3 json files
    Parameters:
      cur - cursor
      conn - connection
    """
    for query in copy_table_queries:
        print(query[:query.find("\n", 1)+1].strip())
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    insert_tables - for all DWH tables execute a SQL INSERT statement to load DWH table from the staging tables
    Parameters:
      cur - cursor
      conn - connection
    """
    for query in insert_table_queries:
        if "(" in query: cutoff = query.find("(") 
        else: cutoff = query.find("\n", 1)
        print(query[:cutoff].strip())
        cur.execute(query)
        conn.commit()
        
        
def count_tables(cur, conn, tag):
    """
    count_tables - for all tables count the number of rows
    Parameters:
      cur - cursor
      conn - connection
      tag - tag string to print as header on output
    """    
    print(tag)
    for query in count_table_queries:
        print(query, end = "")       
        cur.execute(query)
        result = cur.fetchone()
        print(f" - Total records: {result[0]}.")  


def main():
    """ 
    Main function contains core logic for ETL.
    Parameters: refer to function get_input_args above
    """  
    
    start_time = time.time()
    
    # process arguments
    in_args = get_input_args()  
    #print(in_args)

    # process configuration parms
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # connect to database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    if not in_args.skip_count:
        count_tables(cur, conn, "Before ETL>")
    if not in_args.skip_staging:
        load_staging_tables(cur, conn)
    if not in_args.skip_dwh:
        insert_tables(cur, conn)
    if not in_args.skip_count:
       count_tables(cur, conn, "After ETL>")

    conn.close()
    
    tot_time = time.time() - start_time # calculate difference between end time and start time
    print("** Total Elapsed Runtime: " + time.strftime("%H:%M:%S", time.gmtime(tot_time)) )

if __name__ == "__main__":
    main()