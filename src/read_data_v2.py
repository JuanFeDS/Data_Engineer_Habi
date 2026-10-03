"""
Transform for reading and parsing XML property listings.
"""

from typing import Iterator, Dict, Any
import apache_beam as beam
import xml.etree.ElementTree as ET
import logging


class ReadXMLListings(beam.PTransform):
    """
    Transform that reads an XML file and emits each listing as a dictionary.

    This transform reads the entire XML file, parses it, and yields individual
    property listings as dictionaries. Each record is validated to ensure
    it contains the minimum required fields.

    Args:
        file_path: Path to the XML file containing property listings

    Returns:
        PCollection[Dict[str, Any]]: Collection of property listing dictionaries

    Example:
        >>> listings = (
        ...     p
        ...     | 'ReadXML' >> ReadXMLListings('data/feed.xml')
        ... )
    """

    def __init__(self, file_path: str):
        """
        Initialize the XML reader transform.

        Args:
            file_path: Path to the XML file
        """
        super().__init__()
        self.file_path = file_path

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection[Dict[str, Any]]:
        """
        Expand the PTransform to read and parse XML listings.

        Args:
            pcoll: Input PCollection (typically empty, created with beam.Create)

        Returns:
            PCollection containing parsed listing dictionaries
        """
        return (
            pcoll
            | 'InitializePipeline' >> beam.Create([self.file_path])
            | 'ReadAndParseXML' >> beam.FlatMap(self._read_xml_file)
            | 'ValidateRecords' >> beam.Filter(self._is_valid_record)
        )

    @staticmethod
    def _read_xml_file(file_path: str) -> Iterator[Dict[str, Any]]:
        """
        Read the XML file and parse each listing element.

        Args:
            file_path: Path to the XML file

        Yields:
            Dictionary containing parsed listing data

        Raises:
            ET.ParseError: If XML parsing fails
            FileNotFoundError: If the file doesn't exist
        """
        logger = logging.getLogger(__name__)

        try:
            logger.info(f"Reading XML file: {file_path}")
            tree = ET.parse(file_path)
            root = tree.getroot()

            listing_count = 0
            for listing in root.findall('listing'):
                try:
                    yield ReadXMLListings._parse_listing(listing)
                    listing_count += 1
                except Exception as e:
                    logger.warning(f"Error parsing individual listing: {e}")
                    continue

            logger.info(f"Successfully parsed {listing_count} listings from XML")

        except ET.ParseError as e:
            logger.error(f"XML parsing error: {e}")
            raise
        except FileNotFoundError as e:
            logger.error(f"XML file not found: {file_path}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error reading XML: {e}")
            raise

    @staticmethod
    def _parse_listing(listing: ET.Element) -> Dict[str, Any]:
        """
        Extract fields from a single listing XML element.

        Args:
            listing: XML Element representing a property listing

        Returns:
            Dictionary with extracted and typed listing data
        """
        def get_text(element: ET.Element, tag: str, default: str = '') -> str:
            """Safely extract text from XML element."""
            node = element.find(tag)
            return node.text.strip() if node is not None and node.text else default

        def get_float(element: ET.Element, tag: str, default: float = 0.0) -> float:
            """Safely extract float from XML element."""
            text = get_text(element, tag)
            try:
                return float(text) if text else default
            except ValueError:
                return default

        return {
            'state': get_text(listing, 'state'),
            'city': get_text(listing, 'city'),
            'colony': get_text(listing, 'colony'),
            'street': get_text(listing, 'street'),
            'external_num': get_text(listing, 'external_num'),
            'code': get_text(listing, 'code'),
            'type': get_text(listing, 'type'),
            'purpose': get_text(listing, 'purpose'),
            'price': get_float(listing, 'price'),
            'mail_contact': get_text(listing, 'mail_contact').lower(),
            'phone_contact': get_text(listing, 'phone_contact'),
        }

    @staticmethod
    def _is_valid_record(record: Dict[str, Any]) -> bool:
        """
        Validate that a record contains minimum required fields.

        Args:
            record: Listing dictionary to validate

        Returns:
            True if record is valid, False otherwise
        """
        required_fields = ['code', 'mail_contact']
        return all(record.get(field) for field in required_fields)
