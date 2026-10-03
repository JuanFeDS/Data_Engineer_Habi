# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a data engineering ETL project (habi-etl) that extracts real estate property data from XML files, transforms it, and loads it into a MySQL database. The project is designed as a technical challenge that demonstrates pipeline development with Apache Beam and containerization capabilities.

## Architecture

### Two Implementations

The project contains two different implementations:

1. **Apache Beam Pipeline** (`src/` at root level):
   - `src/read_data_ab.py`: Apache Beam DoFn for reading XML data
   - `src/clean_data_ab.py`: Apache Beam DoFn for data cleaning and transformation
   - `src/load_records_ab.py`: Apache Beam DoFn for loading data to MySQL
   - `pipeline.py`: Main Apache Beam pipeline orchestrator
   - Entry point: `pipeline.py`

2. **Traditional Python ETL** (`app/` directory):
   - `app/src/extract.py`: XML extraction logic using Apache Beam transforms
   - `app/main.py`: Simple entry point (currently minimal)
   - Uses PTransform pattern for XML processing

### Data Flow

1. **Extract**: Read XML from `Data/Raw/feed.xml` or `app/data/feed.xml`
2. **Transform**: Clean data and split into two datasets:
   - Users dataset (unique email contacts)
   - Properties dataset (real estate listings with location, type, price, etc.)
3. **Load**: Insert data into MySQL tables:
   - `TBL_DIM_USERS` (dimension table for users)
   - `TBL_FCT_PROPERTIES` (fact table for properties)

### Key Directories

- `src/`: Apache Beam ETL modules (root-level implementation)
- `app/`: Alternative implementation with Apache Beam transforms
- `Utils/`: Shared utilities (logging, database queries via `consultas_ab.py`)
- `exploration/`: Contains `eda.ipynb` for exploratory data analysis
- `Notebooks/`: Contains `02.Business_Case.ipynb` for business analytics
- `tests/`: Test suite with pytest
- `docs/`: Project documentation including `Prueba_Tecnica.md` (technical requirements)
- `bbdd/`: Database-related files (empty directory structure)
- `data/`: Output directory for processed data
- `old_version/`: Previous implementation (reference only)

## Development Commands

### Environment Setup

The project uses `uv` for dependency management:

```bash
# Activate virtual environment (uv creates it automatically)
# The .venv directory is already set up

# Install dependencies
uv sync

# Install development dependencies
uv sync --dev
```

### Running the Pipeline

Apache Beam pipeline (root-level implementation):

```bash
# Main Apache Beam pipeline
python pipeline.py
```

Traditional ETL (legacy, uses non-Beam approach in old_version):

```bash
# Old version main entry point
python main.py
```

App-level implementation:

```bash
# Run app version
python -m app.main
```

### Testing

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=src --cov-report=term-missing

# Run specific test file
uv run pytest tests/test_main.py
```

### Code Quality

```bash
# Format code with Black
uv run black .

# Run linter (pylint available but not enforced in pre-commit yet)
uv run pylint src/

# Run pre-commit hooks manually
pre-commit run --all-files
```

### Jupyter Notebooks

```bash
# Start Jupyter Lab for exploration
uv run jupyter lab

# Main notebooks:
# - exploration/eda.ipynb: Exploratory data analysis
# - Notebooks/02.Business_Case.ipynb: Business analytics queries
```

## Python Version

The project requires Python >= 3.13 (specified in pyproject.toml).

## Database Configuration

The project expects MySQL database connection via environment variables:
- `user`: Database username
- `password`: Database password
- `host`: Database host
- `database`: Database name (e.g., "habi_bbdd")
- `port`: Database port (default 3306)

Use `.env` file (see `old_version/.env` for reference) or Docker environment variables.

## Apache Beam Patterns

### DoFn Pattern

Custom DoFn classes are used for each pipeline stage:

```python
class ReadData(beam.DoFn):
    def __init__(self, logger):
        self.logger = logger

    def process(self, element):
        # Processing logic
        yield result
```

### PTransform Pattern

The app implementation uses PTransform for modular pipeline components:

```python
class ProcessXMLFile(beam.PTransform):
    def expand(self, pcoll):
        return (pcoll
                | 'Step 1' >> beam.Create([None])
                | 'Step 2' >> beam.FlatMap(lambda _: process_data()))
```

### Tagged Outputs

The cleaning stage outputs multiple datasets using tuple tagging:

```python
# In CleanData DoFn
yield ("users_df", users_df)
yield ("properties_df", properties_df)

# Then filter by tag
users_df = cleaned_data | beam.Filter(lambda x: x[0] == "users_df")
```

## Logging

Centralized logging is configured via `Utils.functions.config_logging()`:
- Accepts date parameter for log file naming
- Logs are stored in `Log/` directory (in old_version structure)
- Pass logger to DoFn constructors for consistent logging

## Pre-commit Hooks

Currently configured hooks (in `.pre-commit-config.yaml`):
- **black**: Code formatting (line-length=88)

Commented out but available:
- isort: Import sorting
- pylint: Linting with min score 8.5
- pytest-check: Run tests with 80% coverage requirement

## Key Data Structures

### Raw XML Structure

The feed.xml contains `<listing>` elements with:
- Location: state, city, colony, street, external_num
- Property: code, type, purpose, price
- Contact: mail_contact, phone_contact

### DataFrames

Column naming convention: UPPERCASE (e.g., `STATE`, `CITY`, `PRICE`)

## Business Requirements

The project answers these business questions (see `docs/Prueba_Tecnica.md`):
1. How many users are registered?
2. How many properties per user?
3. How many houses vs apartments per state?
4. Are there duplicate codes? Why?

Queries should be written to answer these questions using the MySQL database.

## Git Workflow

The project uses feature branches:
- Main branch: `main`
- Current feature branch: `feature/EDA` (for exploratory data analysis work)

## Notes

- The project has two parallel implementations (root-level Apache Beam and app-level) that may be in different stages of development
- Use the root-level `pipeline.py` as the primary Apache Beam implementation
- The `old_version/` directory contains a legacy implementation for reference
- When modifying data processing logic, update both the Apache Beam DoFn classes and ensure consistency across implementations
