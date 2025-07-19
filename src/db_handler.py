from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
import datetime

class DBHandler:
    def __init__(self, db_url):
        self.engine = create_engine(db_url)
        self.metadata = MetaData(bind=self.engine)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()

    def insert_transaction(self, row):
        t = Table('affiliate_transactions', self.metadata, autoload_with=self.engine)
        try:
            ins = t.insert().values(**row)
            self.engine.execute(ins)
            return True, ''
        except Exception as e:
            return False, str(e)

    def insert_ingestion_log(self, file_name, network, imported, errored):
        log = Table('ingestion_log', self.metadata, autoload_with=self.engine)
        ins = log.insert().values(
            processed_at=datetime.datetime.now().isoformat(sep=' '), 
            file_name=file_name, 
            network=network, 
            num_rows_imported=imported, 
            num_rows_errored=errored
        )
        self.engine.execute(ins)
