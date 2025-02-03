import yaml
from sqlalchemy import create_engine, inspect

#Connect and upload to db
class DatabaseConnector():
    def __init__(self,credentials):
        self.credentials = credentials

    def read_db_creds(self):
        with open(self.credentials, 'r') as stream:
            db_creds = yaml.safe_load(stream)
            return db_creds


    def init_db_engine(self):
        rds_db = self.read_db_creds()
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = rds_db['RDS_HOST']
        USER = rds_db['RDS_USER']
        PASSWORD = rds_db['RDS_PASSWORD']
        DATABASE = rds_db['RDS_DATABASE']
        PORT = rds_db['RDS_PORT']
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        engine.execution_options(isolation_level='AUTOCOMMIT').connect()
        
        return engine 


    def list_db_tables(self):
        engine = self.init_db_engine()
        rds_tables = inspect(engine)
        return rds_tables.get_table_names()

    def upload_to_db(self,df,table_name):
        self.table_name = table_name
        self.df = df
        try:
            print(f'Pease wait, attempting to upload {table_name} to the local database')
            df.to_sql(table_name,self.init_db_engine(), if_exists='fail',index=False)
            return print(f'{table_name} has been successfully uploaded to the local database')
        except ValueError:
            return print(f'{table_name} is already in the local database')






if __name__ == '__main__':
    pass