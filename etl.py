import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from data_processing import data_processing
from load_tables_to_s3 import load_tables_to_s3_queries


def load_staging_tables(cur, conn):
    '''
    Copies data into staging tables, as defined in 'copy_table_queries'.
    
        Parameters:
            cur: cursor that can be used to execute queries
            conn: creates a connection to the database
        
        Returns:
            None
    '''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
    Insert the relevant data into fact and dimension tables from the staging tables, as defined in 'insert_table_queries'.
    
        Parameters:
            cur: cursor that can be used to execute queries
            conn: creates a connection to the database
        
        Returns:
            None
    '''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()       

        
def load_tables_to_s3(cur, conn):
    '''
    Load the newly created fact and dimension tables into destination S3 bucket.
    
        Parameters:
            cur: cursor that can be used to execute queries
            conn: creates a connection to the database
        
        Returns:
            None
    '''
    for query in load_tables_to_s3_queries:
        cur.execute(query)
        conn.commit()
        
        
def main():
    '''
    Main function that connects to the database through credentials wtihin the config file, and creates a cursor to execute the load_staging_tables and insert_tables functions. Connection is then closed once the pipeline is complete.
    
        Parameters:
            None
        
        Returns:
            None
    ''' 
    config = configparser.ConfigParser()
    config.read('capstone_project.cfg')

    file_input = "/directory-here/raw_data/"
    file_output = "/directory-here/processed_data/"
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    data_processing(file_input, file_output)
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    load_tables_to_s3(cur, conn)
    
    conn.close()
    

if __name__ == "__main__":
    main()
