"""Entry point"""
from datetime import datetime

from Utils.functions import config_logging
from src import read_data, clean_data, load_records
import mysql.connector

# Variables Globales
fecha = datetime.now()
dia = int(str(fecha).replace('-', '')[:8])

# Función principal
def main():
    """General logic"""
    logger = config_logging(dia, append=False)
    logger.info('The execution begins')

    df_raw = read_data.run(fecha)

    df_users, df_properties = clean_data.run(fecha, df_raw)

    load_records.run(fecha, df_users, 'TBL_DIM_USERS')
    # load_records.run(fecha, df_properties, 'TBL_FCT_PROPERTIES')
    logger.info("Data upload completed successfully.")

# Entry Point
if __name__ == '__main__':
    main()
