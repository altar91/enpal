import psycopg2
import os
import sys
import warnings
from configparser import ConfigParser
from psycopg2 import extras

warnings.filterwarnings("ignore")
config = ConfigParser()
config.read('../config.ini')

user=config['scot']['user_name']
password=config['scot']['password']
db_name=config['scot']['db_name']
host=config['scot']['host']
port=config['scot']['port']

params = {'host': host,
          'database': db_name,
          'user': user,
          'password': password,
          'port': port}

def create_table(conn, path):
    f = open(path, 'r')
    sql = f.read()
    f.close()

    commands = (sql)
    try:
        cur = conn.cursor()
        cur.execute(commands)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        cur.close()
        conn.commit()

if __name__ == '__main__':
    try:
        conn = psycopg2.connect(**params)
        path=["./SQL/customer_orders.sql" , "./SQL/bases.sql", "./SQL/teams.sql"]

        for i in range(len(path)):
            create_table(conn, path=path[i])
            print("Created : " + path[i])
    finally:
        if conn is not None:
            conn.close()

