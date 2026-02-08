"General utilites"
import hashlib
import logging

import pandas as pd

def config_logging(dia, append=True):
    """Logging configuration"""

    # Especificamos el formato del logging
    log_format = "%(levelname)s %(asctime)s - %(message)s"

    # Definimos el modo de escritura
    filemode = 'a' if append is True else 'w'

    # Configuramos nuestro logging
    logging.basicConfig(
        filename=f'./Log/log_{dia}.log',
        level=logging.INFO,
        format=log_format,
        filemode=filemode,
        encoding='utf-8'
    )

    # Inicializamos el logging
    logger = logging.getLogger()

    return logger

def generate_numeric_id(
        df: pd.DataFrame,
        dimensions: list,
        id_column: str = "unique_id"
    ) -> pd.DataFrame:
    """
    Genera un ID único numérico basado en n dimensiones de un DataFrame.

    :param df: DataFrame de Pandas con los datos.
    :param dimensions: Lista de nombres de columnas que se usarán para generar el ID.
    :param id_column: Nombre de la nueva columna donde se guardará el ID único.
    :return: DataFrame con la columna de ID único agregada.
    """
    if not all(dim in df.columns for dim in dimensions):
        raise ValueError("Algunas dimensiones no existen en el DataFrame")

    def hash_function(row):
        combined = "_".join(str(row[dim]) for dim in dimensions)  # Concatenar valores
        hash_hex = hashlib.sha256(combined.encode()).hexdigest()  # Generar hash
        return int(hash_hex[:10], 16) % 10**10  # Convertir a número y limitar a 10 dígitos

    df[id_column] = df.apply(hash_function, axis=1)

    return df