from database_utils import DatabaseConnector
import pandas as pd
import tabula
import requests 
import boto3
import os
#Helps extract data from different sources


class DataExtractor:
    def read_rds_database(self):
        self.rds_connection = DatabaseConnector('credentials/db_instance_creds.yaml')

        return self.rds_connection.init_db_engine()
    
    def read_rds_table(self,table_name):
        self.table_name = table_name
        return pd.read_sql_table(table_name,self.read_rds_database())

    def retrieve_pdf_data(self,pdf_link):
        self.pdf_link = pdf_link
        #reading all pdf pages
        pdf_df = tabula.read_pdf(pdf_link, pages='all')
        #Grouping all dfs into one
        pdf_df_concat = pd.concat(pdf_df)
        return pdf_df_concat
    
    def list_number_of_stores(self,endpoint_url,api_cred):
        self.endpoint_url = endpoint_url
        self.api_cred = api_cred
        number_of_stores = requests.get(endpoint_url, headers=api_cred)
        store_number = number_of_stores.json()['number_stores']
        return store_number

    def retrieve_store_data(self,endpoint_url,api_cred,number_of_stores):
        self.endpoint_url = endpoint_url
        self.api_cred = api_cred
        self.list_number_of_stores = number_of_stores
        store_list = []
        store_number = range(0,number_of_stores)

        for i in store_number:
            store_get = requests.get(endpoint_url.format(store_number=store_number[i]),headers=api_cred).json()
            store_list.append(store_get)
        store_data_pd = pd.DataFrame(store_list)
        return store_data_pd
    
    def extract_from_s3(self,s3_address):
        self.s3_address = s3_address
        file_name, file_extension = os.path.splitext(s3_address)
        #splitting address to acquire bucket and key
        s3_address_split = s3_address.split('/')

        bucket_name = s3_address_split[2].split('.')[0]
        key_name = '/'.join(s3_address_split[3:])
        #Using bucket and key in boto3 to acquire data
        s3_connection = boto3.client('s3')
        
        if file_extension == '.csv':
            #downloading s3 bucket object and saving as csv in local directory 
            s3_data = s3_connection.download_file(bucket_name,key_name,'.s3_data.csv')
            s3_df = pd.read_csv('.s3_data.csv',index_col=0)
            return s3_df
        else:
            s3_data = s3_connection.download_file(bucket_name,key_name,'.s3_data.json')
            s3_df = pd.read_json('.s3_data.json')
            return s3_df



            




if __name__ == '__main__':
    pass