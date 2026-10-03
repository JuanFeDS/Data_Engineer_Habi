"""
Main ETL Pipeline for Real Estate Property Data.

This pipeline reads property listings from XML, cleans and transforms data,
splits it into user and property entities, and outputs results.

The pipeline is built using modular, reusable Apache Beam transforms for
maintainability and testability.

Usage:
    python main.py

    # With custom options:
    python main.py --xml_path=/path/to/feed.xml --output_dir=/path/to/output
"""

import logging
import sys
from datetime import datetime
from pathlib import Path

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions

from src.read_data_v2 import ReadXMLListings
from src.clean_data_v2 import CleanListings
from src.load_records_v2 import SplitUsersAndProperties, FormatForDatabase
from config import Config


def setup_logging() -> logging.Logger:
    """
    Configure logging for the pipeline.

    Returns:
        Configured logger instance
    """
    # Ensure log directory exists
    Config.LOG_DIR.mkdir(parents=True, exist_ok=True)

    # Create log file with current date
    date_str = datetime.now().strftime('%Y%m%d')
    log_file = Config.LOG_DIR / f'pipeline_{date_str}.log'

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(levelname)s %(asctime)s [%(name)s] - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )

    return logging.getLogger(__name__)


def run_pipeline(pipeline_options: PipelineOptions) -> None:
    """
    Execute main ETL pipeline.

    This pipeline performs the following steps:
    1. Read and parse XML property listings
    2. Clean and normalize data
    3. Split into user and property entities
    4. Format for output/database
    5. Write results

    Args:
        pipeline_options: Apache Beam pipeline configuration options
    """
    logger = setup_logging()
    logger.info("=" * 80)
    logger.info("Starting Real Estate ETL Pipeline")
    logger.info("=" * 80)

    try:
        # Validate configuration
        Config.validate_config()
        xml_path = Config.get_xml_path()
        logger.info(f"XML input path: {xml_path}")
        logger.info(f"Output directory: {Config.OUTPUT_DIR}")

    except (ValueError, FileNotFoundError) as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)

    # Create and run pipeline
    with beam.Pipeline(options=pipeline_options) as p:

        # Step 1: Read and parse XML listings
        logger.info("Step 1: Reading XML listings...")
        raw_listings = (
            p
            | 'ReadXMLListings' >> ReadXMLListings(xml_path)
        )

        # Step 2: Clean and normalize data
        logger.info("Step 2: Cleaning and normalizing data...")
        clean_listings = (
            raw_listings
            | 'CleanListings' >> CleanListings()
        )

        # Step 3: Split into user and property entities
        logger.info("Step 3: Splitting into users and properties...")
        users, properties = (
            clean_listings
            | 'SplitEntities' >> SplitUsersAndProperties()
        )

        # Step 4: Format for database
        logger.info("Step 4: Formatting for database...")
        formatted_users = (
            users
            | 'FormatUsers' >> FormatForDatabase('users')
        )

        formatted_properties = (
            properties
            | 'FormatProperties' >> FormatForDatabase('properties')
        )

        # Step 5: Write outputs
        logger.info("Step 5: Writing outputs...")

        # Write users to JSONL (one JSON object per line)
        formatted_users | 'WriteUsers' >> beam.io.WriteToText(
            str(Config.USERS_OUTPUT_PATH),
            file_name_suffix='.jsonl',
            shard_name_template='',
            num_shards=1
        )

        # Write properties to JSONL
        formatted_properties | 'WriteProperties' >> beam.io.WriteToText(
            str(Config.PROPERTIES_OUTPUT_PATH),
            file_name_suffix='.jsonl',
            shard_name_template='',
            num_shards=1
        )

    logger.info("=" * 80)
    logger.info("Pipeline completed successfully!")
    logger.info(f"Users output: {Config.USERS_OUTPUT_PATH}.jsonl")
    logger.info(f"Properties output: {Config.PROPERTIES_OUTPUT_PATH}.jsonl")
    logger.info("=" * 80)


def main():
    """
    Main entry point for the pipeline.

    Configures pipeline options and executes the ETL pipeline.
    """
    # Configure pipeline options
    pipeline_options = PipelineOptions()

    # Enable save_main_session for accessing global context
    pipeline_options.view_as(SetupOptions).save_main_session = True
    
    # Use DirectRunner to avoid network issues
    pipeline_options.view_as(StandardOptions).runner = 'DirectRunner'

    # Run pipeline
    run_pipeline(pipeline_options)


if __name__ == '__main__':
    main()
