"""
Centralized configuration for the ETL pipeline.

This module contains all configuration settings, paths, and constants
used throughout the pipeline. This makes it easy to modify settings
without changing code.
"""

import os
from pathlib import Path
from typing import Dict

# Try to load environment variables from .env file if dotenv is available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # dotenv not available, use system environment variables
    pass


class Config:
    """
    Configuration class for ETL pipeline settings.

    Attributes:
        PROJECT_ROOT: Root directory of the project
        DATA_DIR: Directory containing data files
        XML_INPUT_PATH: Path to input XML file
        OUTPUT_DIR: Directory for output files
        LOG_DIR: Directory for log files
        DB_CONFIG: Database connection configuration
    """

    # Project paths
    PROJECT_ROOT = Path(__file__).parent
    DATA_DIR = PROJECT_ROOT / 'Data' / 'Raw'
    OUTPUT_DIR = PROJECT_ROOT / 'data' / 'output'
    LOG_DIR = PROJECT_ROOT / 'Log'

    # Input/Output files
    XML_INPUT_PATH = DATA_DIR / 'feed.xml'
    USERS_OUTPUT_PATH = OUTPUT_DIR / 'users'
    PROPERTIES_OUTPUT_PATH = OUTPUT_DIR / 'properties'

    # Database configuration
    DB_CONFIG: Dict[str, str] = {
        'user': os.getenv('user', 'root'),
        'password': os.getenv('password', ''),
        'host': os.getenv('host', 'localhost'),
        'database': os.getenv('database', 'habi_bbdd'),
        'port': os.getenv('port', '3306')
    }

    # SQL query paths
    SQL_DIR = PROJECT_ROOT / 'SQL' / 'INSERT'
    SQL_USERS_INSERT = SQL_DIR / 'TBL_DIM_USERS.sql'
    SQL_PROPERTIES_INSERT = SQL_DIR / 'insert_properties.sql'

    # Pipeline settings
    PIPELINE_NAME = 'habi-real-estate-etl'
    RUNNER = 'DirectRunner'  # Can be changed to 'DataflowRunner' for cloud execution

    # Data validation settings
    REQUIRED_USER_FIELDS = ['user_id', 'user_mail', 'user_phone']
    REQUIRED_PROPERTY_FIELDS = [
        'property_id', 'state', 'city', 'colony', 'street',
        'external_num', 'user_id', 'type', 'purpose', 'price'
    ]

    # Default values for missing data
    DEFAULT_ADDRESS_PLACEHOLDER = '<--->'
    DEFAULT_PRICE = 0

    @classmethod
    def ensure_directories(cls) -> None:
        """
        Ensure all required directories exist.

        Creates directories if they don't exist:
        - Output directory
        - Log directory
        """
        cls.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        cls.LOG_DIR.mkdir(parents=True, exist_ok=True)

    @classmethod
    def get_xml_path(cls) -> str:
        """
        Get the XML input file path as a string.

        Returns:
            String path to XML file

        Raises:
            FileNotFoundError: If XML file doesn't exist
        """
        if not cls.XML_INPUT_PATH.exists():
            raise FileNotFoundError(f"XML file not found: {cls.XML_INPUT_PATH}")
        return str(cls.XML_INPUT_PATH)

    @classmethod
    def validate_config(cls, check_db: bool = False) -> bool:
        """
        Validate that all required configuration is present and valid.

        Args:
            check_db: Whether to validate database configuration (default: False)

        Returns:
            True if configuration is valid

        Raises:
            ValueError: If configuration is invalid
        """
        # Check XML file exists
        if not cls.XML_INPUT_PATH.exists():
            raise ValueError(f"XML input file not found: {cls.XML_INPUT_PATH}")

        # Optionally check database configuration
        if check_db:
            required_db_keys = ['user', 'password', 'host', 'database', 'port']
            missing_keys = [key for key in required_db_keys if not cls.DB_CONFIG.get(key)]

            if missing_keys:
                raise ValueError(
                    f"Missing database configuration keys: {', '.join(missing_keys)}"
                )

        return True


# Convenience function to get configuration
def get_config() -> Config:
    """
    Get the configuration instance.

    Returns:
        Config instance with all settings
    """
    return Config


# Initialize directories on import
Config.ensure_directories()
