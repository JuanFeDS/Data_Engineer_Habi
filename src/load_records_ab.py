import pandas as pd
import apache_beam as beam
import Utils.consultas_mysql as cst  # Asegúrate de que maneje conexiones aisladas

class LoadData(beam.DoFn):
    def __init__(self, logger, query):
        self.logger = logger
        self.query = query

    def process(self, df_load: pd.DataFrame):
        self.logger.info("Starting data load process...")
        
        try:
            sql_file_path = f'./SQL/INSERT/{self.query}.sql'

            # Leer la consulta SQL
            try:
                with open(sql_file_path, encoding='utf-8') as file:
                    query_insert = file.read()
                self.logger.info("Successfully read SQL query file: %s", sql_file_path)
            except FileNotFoundError:
                self.logger.error("SQL file not found: %s", sql_file_path, exc_info=True)
                raise
            except Exception as e:
                self.logger.error("Error reading SQL file: %s", e, exc_info=True)
                raise

            # Cargar datos en la base de datos
            try:
                if not df_load.empty:  # Asegurar que el DataFrame no esté vacío
                    cst.load_records(df_load, query_insert)  # Cada worker maneja su propia conexión
                    
                    self.logger.info("Data successfully loaded into the database.")
                else:
                    self.logger.warning("DataFrame is empty, skipping database load.")
            except Exception as e:
                self.logger.error("Error loading data into the database: %s", e, exc_info=True)
                raise

        except Exception as e:
            self.logger.critical("Critical failure in data load process: %s", e, exc_info=True)
            raise
