"""
Transform for cleaning and normalizing property listing data.
"""

from typing import Dict, Any
import apache_beam as beam
import hashlib
import logging


class CleanListings(beam.PTransform):
    """
    Transform that cleans and normalizes property listing data.

    This transform performs the following operations on each listing:
    1. Normalizes text fields (lowercase emails, clean phone numbers)
    2. Fills missing values with default placeholders
    3. Generates unique property IDs based on key attributes

    Args:
        None

    Returns:
        PCollection[Dict[str, Any]]: Collection of cleaned listing dictionaries

    Example:
        >>> clean_listings = (
        ...     raw_listings
        ...     | 'CleanData' >> CleanListings()
        ... )
    """

    def expand(self, pcoll: beam.PCollection[Dict]) -> beam.PCollection[Dict]:
        """
        Expand the PTransform to apply cleaning operations.

        Args:
            pcoll: Input PCollection of raw listing dictionaries

        Returns:
            PCollection of cleaned and normalized listing dictionaries
        """
        return (
            pcoll
            | 'NormalizeFields' >> beam.Map(self._normalize_fields)
            | 'FillMissingValues' >> beam.Map(self._fill_missing_values)
            | 'GeneratePropertyID' >> beam.Map(self._generate_property_id)
            | 'ConvertTypes' >> beam.Map(self._convert_types)
        )

    @staticmethod
    def _normalize_fields(record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize field values (email lowercase, phone cleanup, etc.).

        Args:
            record: Raw listing dictionary

        Returns:
            Listing dictionary with normalized fields
        """
        # Normalize email to lowercase
        if record.get('mail_contact'):
            record['mail_contact'] = record['mail_contact'].lower().strip()

        # Clean phone number (remove spaces, dashes, parentheses)
        if record.get('phone_contact'):
            phone = record['phone_contact']
            phone = phone.replace('-', '').replace(' ', '').replace('(', '').replace(')', '')
            record['phone_contact'] = phone

        # Normalize text fields to uppercase (for consistency with DB schema)
        text_fields = ['state', 'city', 'colony', 'street', 'type', 'purpose']
        for field in text_fields:
            if record.get(field):
                record[field] = record[field].upper()

        return record

    @staticmethod
    def _fill_missing_values(record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Fill missing or empty values with default placeholders.

        Args:
            record: Listing dictionary

        Returns:
            Listing dictionary with filled missing values
        """
        # Default values for address fields
        defaults = {
            'colony': '<--->',
            'street': '<--->',
            'external_num': '<--->'
        }

        for field, default_value in defaults.items():
            if not record.get(field) or record[field].strip() == '':
                record[field] = default_value

        # Ensure price is not None
        if record.get('price') is None:
            record['price'] = 0.0

        return record

    @staticmethod
    def _generate_property_id(record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate a unique numeric ID for each property based on key attributes.

        The ID is generated using SHA-256 hash of concatenated key fields,
        ensuring the same property always gets the same ID (deterministic).

        Args:
            record: Listing dictionary

        Returns:
            Listing dictionary with added 'property_id' field
        """
        # Key fields that uniquely identify a property
        id_fields = [
            str(record.get('state', '')),
            str(record.get('city', '')),
            str(record.get('colony', '')),
            str(record.get('street', '')),
            str(record.get('code', '')),
            str(record.get('price', '')),
            str(record.get('phone_contact', ''))
        ]

        # Concatenate fields and generate hash
        combined = '_'.join(id_fields)
        hash_hex = hashlib.sha256(combined.encode()).hexdigest()

        # Convert hash to numeric ID (10 digits)
        property_id = int(hash_hex[:10], 16) % 10**10

        record['property_id'] = property_id

        return record

    @staticmethod
    def _convert_types(record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert field types to match expected database schema.

        Args:
            record: Listing dictionary

        Returns:
            Listing dictionary with properly typed fields
        """
        logger = logging.getLogger(__name__)

        # Convert code to integer (user_id)
        try:
            record['user_id'] = int(record.get('code', 0)) if record.get('code') else 0
        except (ValueError, TypeError) as e:
            logger.warning(f"Error converting code to user_id: {e}")
            record['user_id'] = 0

        # Convert phone to integer
        try:
            phone = record.get('phone_contact', '0')
            # Remove any remaining non-numeric characters
            phone_numeric = ''.join(c for c in phone if c.isdigit())
            record['user_phone'] = int(phone_numeric) if phone_numeric else 0
        except (ValueError, TypeError) as e:
            logger.warning(f"Error converting phone_contact: {e}")
            record['user_phone'] = 0

        # Ensure price is integer (in cents or lowest currency unit)
        try:
            record['price'] = int(float(record.get('price', 0)))
        except (ValueError, TypeError) as e:
            logger.warning(f"Error converting price: {e}")
            record['price'] = 0

        return record
