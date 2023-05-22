import psycopg2
import os
import sys
import warnings
from configparser import ConfigParser
from psycopg2 import extras

warnings.filterwarnings("ignore")
config = ConfigParser()
config.read('../config.ini')

USER = config['scot']['user_name']
PASSWORD = config['scot']['password']
DB_NAME = config['scot']['db_name']
HOST = config['scot']['host']
PORT = config['scot']['port']

PATH = ["./SQL/customer_orders.sql", "./SQL/bases.sql", "./SQL/teams.sql"]

params = {'host': HOST,
          'database': DB_NAME,
          'user': USER,
          'password': PASSWORD,
          'port': PORT}


def create_table(conn, path) -> None:
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

        for i in range(len(PATH)):
            create_table(conn, path=PATH[i])
            print("Created : " + PATH[i])
    finally:
        if conn is not None:
            conn.close()
