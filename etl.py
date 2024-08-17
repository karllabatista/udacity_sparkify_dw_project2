import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
#import boto3

def load_staging_tables(cur, conn):
    print("Loading stagging tables ..")
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
'''

def main():
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    #insert_tables(cur, conn)

    conn.close()
'''    

def check_load_errors():
    # Carregar configuração
    config = configparser.ConfigParser()
    config.read('dwh.cfg')  # Certifique-se de que o arquivo de configuração está correto

    # Extrair detalhes da configuração
    db_params = {
        'host': config['CLUSTER']['HOST'],
        'dbname': config['CLUSTER']['db_name'],
        'user': config['CLUSTER']['db_user'],
        'password': config['CLUSTER']['db_password'],
        'port': config['CLUSTER']['db_port']
    }

    try:
        # Conectar ao banco de dados Redshift
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()

        # Consultar a tabela stl_load_errors
        query = """
        SELECT
            query,
            filename,
            line_number,
            colname
            
        FROM
            stl_load_errors
        ORDER BY
            query DESC
        LIMIT 10;
        """
        cur.execute(query)
        errors = cur.fetchall()

        # Exibir resultados
        for error in errors:
            print(f"Query ID: {error[0]}")
            print(f"Filename: {error[1]}")
            print(f"Line Number: {error[2]}")
            print(f"Column Name: {error[3]}")
            #print(f"Error Severity: {error[4]}")
            #print(f"Error Message: {error[5]}")
            print("-" * 50)

        # Fechar a conexão
        cur.close()
        conn.close()
    
    except psycopg2.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Error: {e}")
def etl():
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    #insert_tables(cur, conn)

    conn.close()
    
if __name__ == "__main__":
    #main()
    etl()
    #check_load_errors()
