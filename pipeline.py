import os
from datetime import datetime

from dotenv import load_dotenv

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from src.read_data_ab import ReadData
from src.clean_data_ab import CleanData  
from src.load_records_ab import LoadData

from Utils.functions import config_logging

load_dotenv()

# Configurar opciones del pipeline
pipeline_options = PipelineOptions()

# Definir la fecha
fecha = datetime.now()
logger = config_logging(int(str(fecha).replace('-', '')[:8]))

# Ejecutar el pipeline
with beam.Pipeline(options=pipeline_options) as p:
    cleaned_data = (
        p
        | "Crear entrada vacía" >> beam.Create([None])
        | "Leer desde XML" >> beam.ParDo(ReadData(logger))
        | "Limpiar y procesar datos" >> beam.ParDo(CleanData(logger))
    )

    # Filtramos cada dataset usando la etiqueta asignada en CleanData
    users_df = (
        cleaned_data
        | "Filtrar users_df" >> beam.Filter(lambda x: x[0] == "users_df")
        | "Extraer users_df" >> beam.Map(lambda x: x[1])
    )

    properties_df = (
        cleaned_data
        | "Filtrar properties_df" >> beam.Filter(lambda x: x[0] == "properties_df")
        | "Extraer properties_df" >> beam.Map(lambda x: x[1])
    )

    # Cargar los datos en la base de datos usando LoadData
    users_df | "Cargar users_df" >> beam.ParDo(LoadData(logger, "TBL_DIM_USERS"))
    properties_df | "Cargar properties_df" >> beam.ParDo(LoadData(logger, "insert_properties"))
