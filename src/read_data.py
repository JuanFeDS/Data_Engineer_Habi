"Script to clean raw data from an XML file and return it as a Pandas DataFrame."
from datetime import datetime
import xml.etree.ElementTree as ET

import pandas as pd

from Utils.functions import config_logging

def run(fecha: datetime):
    """
    Reads an XML file containing property listings, processes the data, 
    and returns a Pandas DataFrame.
    
    Args:
        fecha (str): Date in 'YYYY-MM-DD' format to be used for logging.
    
    Returns:
        pd.DataFrame: Processed data from the XML file.
    
    Raises:
        FileNotFoundError: If the XML file is not found at the specified path.
        ET.ParseError: If there is an issue parsing the XML file.
        Exception: Catches other unexpected errors.
    """
    # Convert date format for logging
    dia = int(str(fecha).replace('-', '')[:8])

    # Set up logging
    logger = config_logging(dia)
    logger.info('Starting data reading process...')

    # Define XML file path
    path = './Data/Raw/feed.xml'

    # Load XML file
    tree = ET.parse(path)
    root = tree.getroot()
    logger.info('XML file successfully loaded.')

    data = []

    # Extract data from XML
    for listing in root.findall("listing"):
        data.append({
            "state": listing.find("state").text,
            "city": listing.find("city").text,
            "colony": listing.find("colony").text,
            "street": listing.find("street").text,
            "external_num": listing.find("external_num").text,
            "code": listing.find("code").text,
            "type": listing.find("type").text,
            "purpose": listing.find("purpose").text,
            "price": float(listing.find("price").text) if listing.find("price").text else None,  # Convert to float
            "mail_contact": listing.find("mail_contact").text,
            "phone_contact": listing.find("phone_contact").text
        })

    # Convert extracted data into a DataFrame
    df_raw = pd.DataFrame(data)
    df_raw.columns = [i.upper() for i in df_raw.columns]

    logger.info(f'Data successfully read with {len(df_raw)} records.')
    
    return df_raw
