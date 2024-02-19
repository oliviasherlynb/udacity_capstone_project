import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    '''
    Drops staging, fact, and dimension tables if they exist, as defined in 'drop_table_queries'.
    
        Parameters:
            cur: cursor that can be used to execute queries
            conn: creates a connection to the database
        
        Returns:
            None
    '''    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
    Creates staging, fact, and dimension tables, as defined in 'create_table_queries'.
    
        Parameters:
            cur: cursor that can be used to execute queries
            conn: creates a connection to the database
        
        Returns:
            None
    ''' 
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    Main function that connects to the database through credentials wtihin the config file, and creates a cursor to execute the drop_tables and create_tables functions. Connection is then closed once tables are created.
    
        Parameters:
            None
        
        Returns:
            None
    ''' 
    config = configparser.ConfigParser()
    config.read('capstone_project.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
