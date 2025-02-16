import os
from datetime import datetime

import xml.etree.ElementTree as ET

import pandas as pd
import apache_beam as beam

from Utils.functions import config_logging

class ReadData(beam.DoFn):

    def __init__(self, logger):
        self.logger = logger

    def process(self, element):

        # Configurar logging
        self.logger.info('Starting data reading process...')

        # Definir la ruta del XML
        path = './Data/Raw/feed.xml'

        if not os.path.exists(path):
            self.logger.error("File not found: %s", path)
            raise FileNotFoundError(f'File not found: {path}')

        try:
            # Cargar XML
            tree = ET.parse(path)
            root = tree.getroot()
            self.logger.info('XML file successfully loaded.')
        except ET.ParseError as e:
            self.logger.error("Error parsing XML file: %s", e)
            raise
        except Exception as e:
            self.logger.error("Unexpected error loading XML: %s", e)
            raise

        data = []

        # Extraer datos del XML
        for listing in root.findall("listing"):
            try:
                data.append({
                    "state": listing.find("state").text,
                    "city": listing.find("city").text,
                    "colony": listing.find("colony").text,
                    "street": listing.find("street").text,
                    "external_num": listing.find("external_num").text,
                    "code": listing.find("code").text,
                    "type": listing.find("type").text,
                    "purpose": listing.find("purpose").text,
                    "price": float(listing.find("price").text) if listing.find("price").text else None, 
                    "mail_contact": listing.find("mail_contact").text,
                    "phone_contact": listing.find("phone_contact").text
                })
            except AttributeError as e:
                self.logger.warning("Missing value in listing: %s", e)

        # Convertir datos en DataFrame
        df_raw = pd.DataFrame(data)
        df_raw.columns = [i.upper() for i in df_raw.columns]

        self.logger.info("Data successfully read with %s records.", len(df_raw))

        yield df_raw  # Esto envía cada registro a la PCollection en Apache Beam
