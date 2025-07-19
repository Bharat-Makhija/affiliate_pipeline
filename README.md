# Affiliate Transaction Data Pipeline

## Overview

This project implements a robust data pipeline to clean, ingest, process, and store affiliate marketing transactions from multiple affiliate networks. It applies configurable commission rules, ensures idempotency, logs ingestion events, and simulates syncing commission data with an external affiliate API.

## Features

- Ingest CSV files from multiple networks with varying schemas
- Normalize data fields into a unified format
- Calculate commissions using a configurable JSON file with base rates and bonus rules
- Persist data using SQLite (easily swappable for other databases such as Postgres or AWS Aurora/Redshift)
- Maintain ingestion and commission logs for auditing and monitoring
- Retry logic with exponential backoff for external API commission sync calls
- Idempotent transactions to avoid duplicates on re-ingestion
- Comprehensive integration tests covering multiple scenarios

## Setup Instructions

### Prerequisites

- Python 3.8+
- `pip` package manager

### Installation

1. Clone this repository:

```bash
git clone https://your-repo-url.git
cd affiliate_pipeline
```

2. Create a virtual environment (optional but recommended):

```bash
python3 -m venv venv
source venv/bin/activate  # Linux/macOS
venv\Scripts\activate     # Windows
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

4. Initialize the SQLite database schema:

```bash
sqlite3 src/db/affiliate.db < src/db/scripts/init_db.sql
```

## Usage

To run the ETL pipeline on your data files:

```bash
python3 -m src.etl --config src/config/commission_config.json --data_dir src/data/
```

The script will:

- Process all CSV files in the specified data directory
- Normalize and calculate commissions
- Insert results into the `affiliate_transactions` table
- Record ingestion stats in `ingestion_log`
- Simulate calls to the external partner API for affiliates with ID `aff_01`
- Log any errors and handle duplicates gracefully

## Testing

Run automated tests with:

```bash
pytest tests/
```

Tests include:

- Valid complete ingestion for a known network CSV.
- Partial ingestion where some rows are malformed.
- Complete failure on totally malformed files.

## Configuration

Edit `config/commission_config.json` to modify commission rates per network, including optional bonus rules. Example:

```json
{
  "networkA": {
    "baseRate": 0.1
  },
  "networkB": {
    "baseRate": 0.12,
    "bonus": {
      "minAmount": 100,
      "additionalRate": 0.02
    }
  },
  "default": {
    "baseRate": 0.05
  }
}
```

## Notes & Extensions

- **Database:** The current implementation uses SQLite for easy local testing. For production, switch to PostgreSQL or AWS Aurora/Redshift by changing the connection string in `src/db_handler.py`.
- **API Sync:** External API calls are simulated. Replace the logic in `src/api_sync.py` with your real endpoints and authentication.
- **Deployment:** Consider packaging or containerization for AWS Lambda, Glue, or ECS deployment.
- **Scalability:** Add support for parallel processing, streaming input, or S3 bucket triggers for full cloud integration.

## Troubleshooting

- **`ModuleNotFoundError: No module named 'src'`**:  
  Ensure you run scripts and tests from the project root directory and that `src/` contains an `__init__.py` file.
- **Database Locked Errors on SQLite:**  
  Avoid concurrent writes; switch to a more robust DB for production.
- **Date parsing errors:**  
  Verify timestamps are valid and supported by `dateutil.parser`.
