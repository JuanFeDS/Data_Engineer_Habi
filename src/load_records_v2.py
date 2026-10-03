"""
Transform for splitting listings into separate user and property entities.
"""

from typing import Dict, Any, Iterator, Tuple
import apache_beam as beam
import logging


class SplitUsersAndProperties(beam.PTransform):
    """
    Transform that splits a stream of listings into two separate streams:
    - Unique users (deduplicated by user_id)
    - Properties (one per listing)

    This transform extracts user information and property information from
    each listing record and outputs them as separate PCollections.

    Returns:
        Tuple of (users_pcoll, properties_pcoll)
            - users_pcoll: PCollection[Dict] with unique user records
            - properties_pcoll: PCollection[Dict] with property records

    Example:
        >>> users, properties = (
        ...     clean_listings
        ...     | 'SplitEntities' >> SplitUsersAndProperties()
        ... )
    """

    # Output tags for tagged outputs
    OUTPUT_TAG_USERS = 'users'
    OUTPUT_TAG_PROPERTIES = 'properties'

    class _ExtractEntities(beam.DoFn):
        """
        DoFn that extracts both user and property entities from a listing.

        Uses tagged outputs to emit multiple entity types from a single input.
        """

        def process(self, record: Dict[str, Any]) -> Iterator[Tuple[str, Dict]]:
            """
            Process a listing record and emit user and property entities.

            Args:
                record: Cleaned listing dictionary

            Yields:
                Tuples of (entity_type, entity_dict) for filtering
            """
            logger = logging.getLogger(__name__)

            try:
                # Extract user entity
                user_entity = {
                    'user_id': record['user_id'],
                    'user_mail': record['mail_contact'],
                    'user_phone': record['user_phone']
                }
                yield ('users', user_entity)

            except KeyError as e:
                logger.warning(f"Missing user field in record: {e}")

            try:
                # Extract property entity
                property_entity = {
                    'property_id': record['property_id'],
                    'state': record['state'],
                    'city': record['city'],
                    'colony': record['colony'],
                    'street': record['street'],
                    'external_num': record['external_num'],
                    'user_id': record['user_id'],
                    'type': record['type'],
                    'purpose': record['purpose'],
                    'price': record['price']
                }
                yield ('properties', property_entity)

            except KeyError as e:
                logger.warning(f"Missing property field in record: {e}")

    def expand(self, pcoll: beam.PCollection[Dict]) -> Tuple[beam.PCollection, beam.PCollection]:
        """
        Expand the PTransform to split entities and deduplicate users.

        Args:
            pcoll: Input PCollection of cleaned listing dictionaries

        Returns:
            Tuple of (unique_users_pcoll, properties_pcoll)
        """
        # Extract both entity types from each listing
        entities = (
            pcoll
            | 'ExtractEntities' >> beam.ParDo(self._ExtractEntities())
        )

        # Split into separate collections
        users = (
            entities
            | 'FilterUsers' >> beam.Filter(lambda x: x[0] == 'users')
            | 'ExtractUserData' >> beam.Map(lambda x: x[1])
        )

        properties = (
            entities
            | 'FilterProperties' >> beam.Filter(lambda x: x[0] == 'properties')
            | 'ExtractPropertyData' >> beam.Map(lambda x: x[1])
        )

        # Deduplicate users by user_id (keep first occurrence)
        unique_users = (
            users
            | 'KeyByUserID' >> beam.Map(lambda u: (u['user_id'], u))
            | 'DeduplicateUsers' >> beam.CombinePerKey(lambda values: next(iter(values)))
            | 'ExtractUserValues' >> beam.Values()
        )

        return unique_users, properties


class FormatForDatabase(beam.PTransform):
    """
    Transform that formats entity dictionaries for database insertion.

    Ensures all fields are in the correct order and format expected by
    SQL INSERT statements.

    Args:
        entity_type: Either 'users' or 'properties'

    Returns:
        PCollection[Dict] with fields ordered for database insertion
    """

    def __init__(self, entity_type: str):
        """
        Initialize the formatter.

        Args:
            entity_type: Type of entity ('users' or 'properties')
        """
        super().__init__()
        if entity_type not in ['users', 'properties']:
            raise ValueError("entity_type must be 'users' or 'properties'")
        self.entity_type = entity_type

    def expand(self, pcoll: beam.PCollection[Dict]) -> beam.PCollection[Dict]:
        """
        Format entities for database insertion.

        Args:
            pcoll: Input PCollection of entity dictionaries

        Returns:
            PCollection of formatted entity dictionaries
        """
        if self.entity_type == 'users':
            return pcoll | 'FormatUsers' >> beam.Map(self._format_user)
        else:
            return pcoll | 'FormatProperties' >> beam.Map(self._format_property)

    @staticmethod
    def _format_user(user: Dict[str, Any]) -> Dict[str, Any]:
        """
        Format user entity with fields in database order.

        Args:
            user: User dictionary

        Returns:
            Formatted user dictionary
        """
        return {
            'USER_ID': user['user_id'],
            'USER_MAIL': user['user_mail'],
            'USER_PHONE': user['user_phone']
        }

    @staticmethod
    def _format_property(prop: Dict[str, Any]) -> Dict[str, Any]:
        """
        Format property entity with fields in database order.

        Args:
            prop: Property dictionary

        Returns:
            Formatted property dictionary
        """
        return {
            'PROPERTIE_ID': prop['property_id'],
            'STATE': prop['state'],
            'CITY': prop['city'],
            'COLONY': prop['colony'],
            'STREET': prop['street'],
            'EXTERNAL_NUM': prop['external_num'],
            'USER_ID': prop['user_id'],
            'TYPE': prop['type'],
            'PURPOSE': prop['purpose'],
            'PRICE': prop['price']
        }
