import os
import pandas as pd

from src.commission import load_commission_config, calculate_commission
from src.db_handler import DBHandler
from src.utils import clean_amount, normalize_timestamp
from src.api_sync import sync_commission
from src.config.constants import DB_URL

FIELD_MAPS = {
    'network_A.csv': {
        'transaction_id': 'id',
        'affiliate_id': 'aff_id',
        'amount': 'value',
        'timestamp': 'created_at',
        'network': 'networkA'
    },
    'network_B.csv': {
        'transaction_id': 'transaction',
        'affiliate_id': 'affiliate',
        'amount': 'amount',
        'timestamp': 'timestamp',
        'network': 'networkB'
    }
}

def main(config, data_dir, db_url=DB_URL):
    cfg = load_commission_config(config)
    db = DBHandler(db_url)
    files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]
    for fname in files:
        net_map = FIELD_MAPS.get(fname)
        if not net_map:
            print(f"Unknown network for file {fname}")
            continue
        df = pd.read_csv(os.path.join(data_dir, fname))
        imported = errored = 0
        for i, row in df.iterrows():
            try:
                std_row = {
                    'transaction_id': str(row[net_map['transaction_id']]),
                    'affiliate_id': str(row[net_map['affiliate_id']]),
                    'network': net_map['network'],
                    'amount': clean_amount(row[net_map['amount']]),
                    'timestamp': normalize_timestamp(row[net_map['timestamp']]),
                }
                std_row['commission'] = calculate_commission(
                    net_map['network'], std_row['amount'], cfg
                )
                ok, err = db.insert_transaction(std_row)
                if not ok:
                    if "UNIQUE constraint failed" in err:
                        continue  # Already exists (idempotent)
                    errored += 1
                    continue
                imported += 1

                if std_row['affiliate_id'] == 'aff_01':
                    sync_commission({
                        'transaction_id': std_row['transaction_id'],
                        'amount': std_row['amount'],
                        'commission': std_row['commission']
                    })

            except Exception as e:
                errored += 1
                print(f"Error processing row: {row} ({e})")
        db.insert_ingestion_log(fname, net_map['network'], imported, errored)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', default='./src/config/commission_config.json')
    parser.add_argument('--data_dir', default='./src/data/')
    args = parser.parse_args()
    main(args.config, args.data_dir)
