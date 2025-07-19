CREATE TABLE IF NOT EXISTS affiliate_transactions (
    transaction_id TEXT PRIMARY KEY,
    timestamp TEXT NOT NULL,
    affiliate_id TEXT NOT NULL,
    network TEXT NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    commission DECIMAL(10,2) NOT NULL
);

CREATE TABLE IF NOT EXISTS ingestion_log (
    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
    processed_at TEXT NOT NULL,
    file_name TEXT NOT NULL,
    network TEXT NOT NULL,
    num_rows_imported INTEGER NOT NULL,
    num_rows_errored INTEGER NOT NULL
);

