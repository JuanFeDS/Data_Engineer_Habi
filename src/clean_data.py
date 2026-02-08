"Module to clean data and transform it into structured datasets."
from datetime import datetime
import pandas as pd
from Utils.functions import config_logging, generate_numeric_id

def run(fecha: datetime, df_raw: pd.DataFrame):
    """
    Cleans raw data by generating unique property IDs, formatting user and property data,
    handling missing values, and adjusting data types.

    Args:
        fecha (datetime): Date of the execution, used for logging.
        df_raw (pd.DataFrame): Raw data containing property and user information.

    Returns:
        tuple: Cleaned dataframes (df_users, df_properties).
    """
    # Initialize logger
    dia = int(str(fecha).replace('-', '')[:8])
    logger = config_logging(dia)
    logger.info("Starting data cleaning process...")

    # Generate unique property IDs
    df_raw = generate_numeric_id(
        df_raw,
        ['STATE', 'CITY', 'COLONY', 'STREET', 'CODE', 'PRICE', 'PHONE_CONTACT'],
        'PROPERTIE_ID'
    )
    logger.info("Generated unique property IDs.")
    df_raw['USER_ID'] = pd.to_numeric(
        df_raw.get('CODE'),
        errors='coerce'
    ).fillna(0).astype(int)

    df_raw['USER_PHONE'] = pd.to_numeric(
        df_raw.get('PHONE_CONTACT'),
        errors='coerce'
    ).fillna(0).astype(int)

    df_raw['PRICE'] = pd.to_numeric(
        df_raw.get('PRICE'),
        errors='coerce'
    ).fillna(0).astype(int)

    logger.info("Converted data types successfully.")

    # Process Users DataFrame
    df_users = df_raw[['USER_ID', 'MAIL_CONTACT', 'USER_PHONE']].copy()
    df_users.rename(columns={'MAIL_CONTACT': 'USER_MAIL'}, inplace=True)
    logger.info("Users data cleaned and formatted.")

    # Process Properties DataFrame
    df_properties = df_raw[[
        'PROPERTIE_ID', 'STATE', 'CITY', 'COLONY', 'STREET', 'EXTERNAL_NUM', 'USER_ID',
        'TYPE', 'PURPOSE', 'PRICE'
    ]].copy()

    df_properties.fillna({
        'COLONY': '<--->', 
        'STREET': '<--->', 
        'EXTERNAL_NUM': '<--->'
    }, inplace=True)
    logger.info("Properties data cleaned and formatted.")

    return df_users, df_properties
