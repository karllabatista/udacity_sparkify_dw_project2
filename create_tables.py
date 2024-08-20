import configparser
import psycopg2

from sql_queries import create_table_queries,drop_table_queries

def drop_tables(cur, conn):
    """Drop tables on database

    Parameters
    ---------

    cur:psycopg2.extensions.connection
        Manager the connection with Redshift 

    conn:sycopg2.extensions.cursor
        Execute SQL commands and retrieve resulst from database

    Returns:
    -------
    None

    """
    
    try:
        print("Dropping tables ..")
        for query in drop_table_queries:
            print(f"Processing {query}...")
            cur.execute(query)
            conn.commit()
            print("Table dropped successfully!")
        
        print("Tables dropped successfully")
  
    except psycopg2.Error as error:
        print("An unexpected error in Database occurred:",error)
         
    except Exception as error:   
        print("An unexpected error occurred:",error) 
        
   


def create_tables(cur, conn):
    """Create a new tables on database


    Parameters
    ---------

    cur:psycopg2.extensions.connection
        Manager the connection with Redshift 

    conn:sycopg2.extensions.cursor
        Execute SQL commands and retrieve resulst from database

    Returns:
    -------
    None
    """
    try:
        print("Creating tables ..")
        for query in create_table_queries:
            print(f"Processing {query}...")
            cur.execute(query)
            conn.commit()
            print("Table created successfully!")

        print("Finished Creating Tables!")
    
    except psycopg2.Error as error:
        print("An unexpected error in Database occurred:",error) 

    except Exception as error:   
        print("An unexpected error occurred:",error) 



def main():
    """ Manager create and drop tables on database

    Parameters
    ---------
    None
    
    Returns:
    -------
    None

    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
   
    
    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        print(conn)
        cur = conn.cursor()
        
        drop_tables(cur, conn)
        create_tables(cur, conn)
    
	    
    except (psycopg2.DatabaseError, Exception) as error:
        print("Connection failed!! Error:", error)
    
    finally:
        conn.close()
        cur.close()

if __name__ == "__main__":
    main()
 