from datetime import datetime

import pandas as pd
# import mysql.connector

from Utils.functions import config_logging
from src.conexion_sql import conexion

def run(fecha: datetime, df_load: pd.DataFrame, query: str):
    """
    Loads records into the database using a SQL query file.
    
    Args:
        fecha (datetime): Date of the execution, used for logging.
        df_load (pd.DataFrame): Data to be inserted.
        query (str): Name of the SQL file (without extension) containing the insert query.
    """
    dia = int(str(fecha).replace('-', '')[:8])
    logger = config_logging(dia)
    logger.info("Starting data load process...")
    sql_file_path = f'./SQL/INSERT/{query}.sql'

    with open(sql_file_path, encoding='utf-8') as file:
        query_insert = file.read()
    logger.info("Successfully read SQL query file: %s", sql_file_path)

    conexion(df_load, query_insert)
    logger.info("Data successfully loaded into the database.")
