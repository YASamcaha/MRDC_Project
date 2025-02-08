
from sqlalchemy import create_engine, inspect
import yaml

#Connect and upload to db
class DatabaseConnector():
    def __init__(self,credentials):
        self.credentials = credentials

    def read_db_creds(self):
        '''
        Purpose:
            Reads provided credentials to be used in creating a database_engine 
        
        Return:
            Readable credential file in the form of a dictionary
        '''
        with open(self.credentials, 'r') as stream:
            db_creds = yaml.safe_load(stream)
            return db_creds


    def init_db_engine(self):
        '''
        Purpose:
            Uses provided credentials to create an engine to connect to a database instance
        Return:
            Usable database engine 
        '''
        #Reading the provided credentials
        rds_db = self.read_db_creds()
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = rds_db['RDS_HOST']
        USER = rds_db['RDS_USER']
        PASSWORD = rds_db['RDS_PASSWORD']
        DATABASE = rds_db['RDS_DATABASE']
        PORT = rds_db['RDS_PORT']
        #Creating the engine using the information from the rds instance
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        engine.execution_options(isolation_level='AUTOCOMMIT').connect()
        
        return engine 


    def list_db_tables(self):
        '''
        Purpose:
            Inspects the rds engine contents
        Return:
            Provides a list of all table names within the rds
        '''
        engine = self.init_db_engine()
        #Inspecting the engine to return the rds table names
        rds_tables = inspect(engine)
        return rds_tables.get_table_names()

    def upload_to_db(self,df,table_name):
        '''
        Purpose:
            Uploads data to the local database
        Arguments:
            df: The dataframe that is being uploaded
            table_name: The desired name that will be used when the df is uploaded
        Return:
            Confirmation if the table was successfully uploaded or not
        '''
        self.table_name = table_name
        self.df = df
        try:
            print(f'Pease wait, attempting to upload {table_name} to the local database')
            #Transforming the dataframe into a sql object before being pushed to the local database
            df.to_sql(table_name,self.init_db_engine(), if_exists='fail',index=False)
            return print(f'{table_name} has been successfully uploaded to the local database')
        #In case the table already exists the function will skip the upload and return a statement
        except ValueError:
            return print(f'{table_name} is already in the local database')






if __name__ == '__main__':
    print('Please run mrdc_main.py instead, database_utils is used to provide functionality only ')