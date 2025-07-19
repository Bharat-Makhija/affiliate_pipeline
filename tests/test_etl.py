import os
import sqlite3
import pytest
import tempfile
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src import etl
from tests.config.constants import DB_URL

def cleanup_db():
    if os.path.exists('tests/db/affiliate_test.db'):
        os.remove('tests/db/affiliate_test.db')

def initialize_db():
    conn = sqlite3.connect('tests/db/affiliate_test.db')
    with open("src/db/scripts/init_db.sql") as f:
        conn.executescript(f.read())
    conn.close()

@pytest.fixture
def temp_data_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir

def write_csv(data, path):
    with open(path, "w") as f:
        f.write(data)

VALID_CSV = """id,aff_id,value,created_at
tx1001,aff_01,120.50,2025-01-01 10:00:00
tx1002,aff_02,230.00,2025-01-01 10:12:00
"""

def test_successful_ingestion(temp_data_dir):
    # Prepare test data file
    initialize_db()
    csv_path = os.path.join(temp_data_dir, 'network_A.csv')
    write_csv(VALID_CSV, csv_path)

    # Run the pipeline
    etl.main('src/config/commission_config.json', temp_data_dir, DB_URL)

    # Connect to the DB and assert results
    conn = sqlite3.connect('tests/db/affiliate_test.db')
    cursor = conn.cursor()

    # Check 2 transactions are loaded
    cursor.execute("SELECT COUNT(*) FROM affiliate_transactions")
    assert cursor.fetchone()[0] == 2

    # Check ingestion log
    cursor.execute("SELECT num_rows_imported, num_rows_errored FROM ingestion_log ORDER BY log_id DESC LIMIT 1")
    imported, errored = cursor.fetchone()
    assert imported == 2
    assert errored == 0
    conn.close()
    cleanup_db()

PARTIAL_CSV = """id,aff_id,value,created_at
tx2001,aff_01,99.99,2025-01-01 11:00:00
tx2002,aff_02,not_a_number,2025-01-01 11:15:00
tx2003,aff_03,150.75,bad_timestamp
tx2004,aff_04,89.00,2025-01-01 11:30:00
"""

def test_partial_ingestion(temp_data_dir):
    initialize_db()
    csv_path = os.path.join(temp_data_dir, 'network_A.csv')
    write_csv(PARTIAL_CSV, csv_path)

    etl.main('src/config/commission_config.json', temp_data_dir, DB_URL)

    conn = sqlite3.connect('tests/db/affiliate_test.db')
    cursor = conn.cursor()

    # Check only the valid rows were imported (expect 2: tx2001, tx2004)
    cursor.execute("SELECT transaction_id FROM affiliate_transactions ORDER BY transaction_id")
    rows = cursor.fetchall()
    imported_ids = set(row[0] for row in rows)
    assert imported_ids == {'tx2001', 'tx2004'}

    # Check log reflects partial success
    cursor.execute("SELECT num_rows_imported, num_rows_errored FROM ingestion_log ORDER BY log_id DESC LIMIT 1")
    imported, errored = cursor.fetchone()
    assert imported == 2
    assert errored == 2
    conn.close()
    cleanup_db()

MALFORMED_CSV = """id,aff_id,value,created_at
bad1,aff_01,notanumber,wrongformat
bad2,aff_03,word,alsowrong
"""

def test_malformed_file(temp_data_dir):
    initialize_db()
    csv_path = os.path.join(temp_data_dir, 'network_A.csv')
    write_csv(MALFORMED_CSV, csv_path)

    etl.main('src/config/commission_config.json', temp_data_dir, DB_URL)

    conn = sqlite3.connect('tests/db/affiliate_test.db')
    cursor = conn.cursor()

    # Check no transactions were imported
    cursor.execute("SELECT COUNT(*) FROM affiliate_transactions")
    assert cursor.fetchone()[0] == 0

    # Check all rows errored
    cursor.execute("SELECT num_rows_imported, num_rows_errored FROM ingestion_log ORDER BY log_id DESC LIMIT 1")
    imported, errored = cursor.fetchone()
    assert imported == 0
    assert errored == 2
    conn.close()
    cleanup_db()
