# Utils/consultas_mysql.py
import os
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

# Variables globales con configuración
config = {
    "user": os.getenv('user'),
    "password": os.getenv('password'),
    "host": os.getenv('host'),
    "database": os.getenv('database'),
    "port": os.getenv('port')
}

def get_connection():
    """Devuelve una nueva conexión a la base de datos."""
    return mysql.connector.connect(**config)

def load_records(df, query_insert, conn):
    """Carga registros en la base de datos usando una conexión pasada como argumento."""
    cursor = conn.cursor()
    data = [tuple(row) for row in df.itertuples(index=False, name=None)]
    cursor.executemany(query_insert, data)
    conn.commit()
    cursor.close()
