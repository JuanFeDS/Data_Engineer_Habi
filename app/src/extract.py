import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

import xml.etree.ElementTree as ET
import csv

def read_and_parse_entire_file(file_path):
    """
    Lee el archivo XML completo y extrae todos los listings
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()
        
        # Parsear el XML completo
        root = ET.fromstring(content)
        
        # Extraer todos los elementos listing
        listings = root.findall('listing')
        
        for listing in listings:
            result = parse_listing_element(listing)
            if result:
                yield result
                
    except Exception as e:
        print(f"Error procesando archivo: {e}")

def parse_listing_element(listing_element):
    """
    Extrae los datos de un elemento listing
    """
    try:
        state = listing_element.findtext("state", "").strip()
        city = listing_element.findtext("city", "").strip()
        colony = listing_element.findtext("colony", "").strip()
        street = listing_element.findtext("street", "").strip()
        external_num = listing_element.findtext("external_num", "").strip()
        code = listing_element.findtext("code", "").strip()
        type = listing_element.findtext("type", "").strip()
        purpose = listing_element.findtext("purpose", "").strip()
        price = listing_element.findtext("price", "").strip()
        mail_contact = listing_element.findtext("mail_contact", "").strip().lower()
        phone_contact = listing_element.findtext("phone_contact", "").strip()

        return [
            state,
            city,
            colony,
            street,
            external_num,
            code,
            type,
            purpose,
            float(price) if price else 0.0,
            mail_contact,
            phone_contact
        ]
    except Exception as e:
        print(f"Error procesando listing: {e}")
        return None

def format_csv_row(row):
    """
    Convierte una lista de valores a una cadena CSV
    """
    return ','.join(str(field) if field is not None else '' for field in row)

class ProcessXMLFile(beam.PTransform):
    """
    Transform personalizada para procesar el archivo XML completo
    """
    def expand(self, pcoll):
        file_path = 'C:/Users/jmart/Documents/Proyectos/Data_Science/06_Proyectos/prueba_habi/app/data/feed.xml'
        
        return (pcoll
                | 'Create Single Element' >> beam.Create([None])
                | 'Process XML File' >> beam.FlatMap(lambda _: read_and_parse_entire_file(file_path))
                | 'Filter None' >> beam.Filter(lambda x: x is not None))

def run_pipeline():
    options = PipelineOptions()
    
    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | 'Process XML' >> ProcessXMLFile()
            | 'Format CSV' >> beam.Map(format_csv_row)
            | 'Write to CSV' >> beam.io.WriteToText(
                'data/output', 
                file_name_suffix='.csv',
                shard_name_template='',
                header='state,city,colony,street,external_num,code,type,purpose,price,mail_contact,phone_contact'
            )
        )

if __name__ == '__main__':
    run_pipeline()