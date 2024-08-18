import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    print("Loading stagging tables ..")
    try:
        
        for query in copy_table_queries:
            cur.execute(query)
            conn.commit()
        print("Load completed successfully!!!")

    except Exception as error:
        print(f"Error to load staging tables:{error}")


def insert_tables(cur, conn):
    print("Inserting data ...")
    try:
        for query in insert_table_queries:
            cur.execute(query)
            conn.commit()
        print("Data inserting completed successfully!!!")
    except Exception as error:
        print(f"An error occurred:{error}")

def main():
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
        
        load_staging_tables(cur, conn)
        insert_tables(cur, conn)

    except Exception as error:
        print(f"Error during database operation: {error}")

    finally:
        conn.close()
        print("Connection closed.")


    
if __name__ == "__main__":
    main()
    
