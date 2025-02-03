from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
from data_extraction import DataExtractor 


#Instantiate classes 
db_connector_local = DatabaseConnector('credentials/local_db_creds.yaml')
data_cleaner = DataCleaning()
data_extractor = DataExtractor()


#Using DataExtractor to extract needed RDS data
user_df = data_extractor.read_rds_table('legacy_users')

#Cleaning RDS data
clean_user_df = data_cleaner.clean_user_data(user_df)

#Pushing cleaned RDS data to local database
db_connector_local.upload_to_db(clean_user_df,'dim_users')

#Using DataExtractor to extract needed PDF data
card_df = data_extractor.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")

#Cleaning PDF data
clean_card_df = data_cleaner.clean_card_data(card_df)

#Pushing cleaned card data to local database
db_connector_local.upload_to_db(clean_card_df,'dim_card_details')

#Using DataExtractor to extract store numbers and store data
api_connector = DatabaseConnector('credentials/api_key.yaml').read_db_creds()
number_of_stores = data_extractor.list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores',api_connector)

stores_df = data_extractor.retrieve_store_data('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}',api_connector,number_of_stores)

#Cleaning stores data
clean_stores_df = data_cleaner.clean_store_data(stores_df)

#Pushing cleaned stores data to local database
db_connector_local.upload_to_db(clean_stores_df,'dim_store_details')

#Using DataExtractor to extract product details data
product_df = data_extractor.extract_from_s3('s3://data-handling-public/products.csv')

#Cleaning product details data
clean_product_df = data_cleaner.clean_products_data(product_df)

#Pushing cleaned products data to local database
db_connector_local.upload_to_db(clean_product_df,'dim_products')

#Using DataExtractor to extract orders data
orders_df = data_extractor.read_rds_table('orders_table')

#Cleaning orders data
clean_orders_df = data_cleaner.clean_orders_data(orders_df)

#Pushing cleaned orders data to local database
db_connector_local.upload_to_db(clean_orders_df,'orders_table')

#Using DataExtractor to extract date_time data
date_df = data_extractor.extract_from_s3('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')

#Cleaning date_time data
clean_date_df = data_cleaner.clean_date_times_data(date_df)

#Pushing cleaned date_time data to local database
db_connector_local.upload_to_db(clean_date_df,'dim_date_times')


