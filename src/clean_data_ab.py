import apache_beam as beam
from apache_beam.pvalue import TaggedOutput
import pandas as pd

from Utils.functions import generate_numeric_id

class CleanData(beam.DoFn):
    OUTPUT_TAG_USERS = 'users'
    OUTPUT_TAG_PROPERTIES = 'properties'

    def __init__(self, logger):
        self.logger = logger


    def process(self, df_raw: pd.DataFrame):
        self.logger.info("Starting data cleaning process...")

        try:
            # Initialize logger
            self.logger.info("Starting data cleaning process...")

            # Generate unique property IDs
            try:
                df_raw = generate_numeric_id(
                    df_raw,
                    ['STATE', 'CITY', 'COLONY', 'STREET', 'CODE', 'PRICE', 'PHONE_CONTACT'],
                    'PROPERTIE_ID'
                )
                self.logger.info("Generated unique property IDs.")
            except Exception as e:
                self.logger.error("Error generating unique property IDs: %s", e, exc_info=True)
                raise

            # Convert types safely
            try:
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

                self.logger.info("Converted data types successfully.")
            except Exception as e:
                self.logger.error("Error converting data types: %s", e, exc_info=True)
                raise

            # Process Users DataFrame
            try:
                df_users = df_raw[['USER_ID', 'MAIL_CONTACT', 'USER_PHONE']].copy()
                df_users.rename(columns={'MAIL_CONTACT': 'USER_MAIL'}, inplace=True)
                self.logger.info("Users data cleaned and formatted.")
                yield ('users_df', df_users)
            except KeyError as e:
                self.logger.error("Missing column in user data: %s", e, exc_info=True)
                raise
            except Exception as e:
                self.logger.error("Error processing user data: %s", e, exc_info=True)
                raise

            # Process Properties DataFrame
            try:
                df_properties = df_raw[[
                    'PROPERTIE_ID', 'STATE', 'CITY', 'COLONY', 'STREET', 'EXTERNAL_NUM', 'USER_ID',
                    'TYPE', 'PURPOSE', 'PRICE'
                ]].copy()

                df_properties.fillna({
                    'COLONY': '<--->', 
                    'STREET': '<--->', 
                    'EXTERNAL_NUM': '<--->'
                }, inplace=True)
                self.logger.info("Properties data cleaned and formatted.")
                yield ('properties_df', df_properties)
            except KeyError as e:
                self.logger.error("Missing column in properties data: %s", e, exc_info=True)
                raise
            except Exception as e:
                self.logger.error("Error processing properties data: %s", e, exc_info=True)
                raise

        except Exception as e:
            self.logger.critical("Critical failure in data cleaning: %s", e, exc_info=True)

            yield ('users_df', None)
            yield ('properties_df', None)