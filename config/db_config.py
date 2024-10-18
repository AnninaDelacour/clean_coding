import psycopg2
import os

database_configs = {
    'BronzeData': {
        'dbname': os.getenv('BRONZE_DB'),
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'host': os.getenv('POSTGRES_HOST'),
        'port': os.getenv('POSTGRES_PORT'),
    },
    'SilverData': {
        'dbname': os.getenv('SILVER_DB'),
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'host': os.getenv('POSTGRES_HOST'),
        'port': os.getenv('POSTGRES_PORT'),
    },
    'GoldData': {
        'dbname': os.getenv('GOLD_DB'),
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'host': os.getenv('POSTGRES_HOST'),
        'port': os.getenv('POSTGRES_PORT'),
    },
}

def connect_to_database(database_name):
    config = database_configs[database_name]
    return psycopg2.connect(
        dbname=config['dbname'],
        user=config['user'],
        password=config['password'],
        host=config['host'],
        port=config['port']
    )

if __name__ == '__main__':
    conn = connect_to_database('BronzeDB')
    print("Conncetion to BronzeDB was successful!")
    conn.close()
    conn = connect_to_database('SilverDB')
    print("Conncetion to SilverDB was successful!")
    conn.close()
    conn = connect_to_database('GoldDB')
    print("Conncetion to GoldDB was successful!")
    conn.close()
