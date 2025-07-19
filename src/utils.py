import re
from dateutil import parser as date_parser

def clean_amount(amount_str):
    if isinstance(amount_str, (float, int)):
        return float(amount_str)
    return float(re.sub(r'[^\d.]', '', str(amount_str)))

def normalize_timestamp(ts_str):
    return date_parser.parse(ts_str).isoformat(sep=' ')
