import os
from dotenv import load_dotenv

import mysql.connector

load_dotenv()

config = {
    "user": os.getenv('user'),
    "password": os.getenv('password'),
    "host": os.getenv('host'),  
    "database": os.getenv('database'),
    "port": os.getenv('port')
}


def conexion(df, query):
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()

    data = [tuple(row) for row in df.itertuples(index=False, name=None)]
    cursor.executemany(query, data)
    conn.commit()

    cursor.close()
    conn.close()
