
import numpy as np
import pandas as pd
from dateutil.parser import parse




#Clean data from different sources
class DataCleaning:
    def clean_user_data(self,user_table):
        self.user_table = user_table
        #Creating a copy of the dataframe
        df_working = user_table
        #**Change "NULL" strings data type into NULL data type**
        df_working.replace({'NULL': None}, inplace=True)

        #Dropping NULL values
        df_working.dropna(axis=0, inplace=True)

        #Finding additional NULL values by cleaning country_code
        country_code_regex = '^(?!.*\d)'
        df_working.loc[~df_working['country_code'].str.match(country_code_regex),'country_code'] = np.nan

        #Dropping NULL values
        df_working.dropna(axis=0, inplace=True)

        #Converting Join_date column to Datetime data type using parse
        df_working['join_date'] = df_working['join_date'].apply(parse)
        df_working['join_date'] = pd.to_datetime(df_working['join_date'],errors='coerce')

        #resetting index after cleaning process
        df_working.reset_index(drop=True)

        return df_working
    
    def clean_card_data(self,card_details):
        self.card_details = card_details
        #Creating a copy of the dataframe
        card_df = card_details.copy()
        #**Change "NULL" strings data type into NULL data type**
        card_df.replace({'NULL': None}, inplace=True)
        #Dropping NULL values
        card_df.dropna(axis=0, inplace=True)
        #Remove duplicate card numbers
        card_df = card_df.drop_duplicates()
        #Removing unwanted special characters from card
        card_df['card_number'] = card_df['card_number'].replace('\?','',regex=True)
        #Removing non numeric card numbers
        card_df.card_number = pd.to_numeric(card_df.card_number, errors='coerce')
        #removing resultant NULL values
        card_df.dropna(axis=0, inplace=True)
        #Setting the card_Details column as int
        card_df['card_number'] = card_df['card_number'].astype('int64')
        #Converting date_payment_confirmed column to Datetime data type using parse
        card_df['date_payment_confirmed'] = card_df['date_payment_confirmed'].apply(parse)
        card_df['date_payment_confirmed'] = pd.to_datetime(card_df['date_payment_confirmed'],errors='coerce')

        return card_df 
    
    def clean_store_data(self,store_data):
        self.store_data = store_data 
        #Creating a copy of the dataframe
        store_df = store_data.copy()
        #**Change "NULL" strings data type into NULL data type**
        store_df.replace(['NULL'],None, inplace=True)
        #Dropping NULL values
        store_df.dropna(axis=0,thresh=2, inplace=True)
        #Finding additional NULL values by cleaning country_code
        regex = '^(?!.*\d)'
        try:
            store_df.loc[~store_df['continent'].str.match(regex),'continent'] = np.nan
            store_df.loc[~store_df['store_type'].str.match(regex),'store_type'] = np.nan
        except TypeError:
            pass

        #Dropping NULL values
        store_df.dropna(axis=0,thresh=11, inplace=True)
        #Converting opening_date column to Datetime data type using parse
        try:
            store_df['opening_date'] = store_df['opening_date'].apply(parse)
        except TypeError:
            pass
        store_df['opening_date'] = pd.to_datetime(store_df['opening_date'],errors='coerce')
        #cleaning staff_numbers
        #Removing all non numeric values
        store_df['staff_numbers'] = store_df['staff_numbers'].replace('[^0-9]','',regex=True)

        #Trimming whitespace
        store_df['staff_numbers'] = store_df['staff_numbers'].str.strip()

        #Casting to int
        store_df['staff_numbers'] = store_df['staff_numbers'].astype('int64')

        return store_df
    

    def convert_product_weight(self,products_data):
        self.products_data = products_data
        df_working = products_data
        #Stripping unwanted elements at the end of the weight column string
        df_working['weight'] = df_working['weight'].astype(str).str.strip('.')
        #Creation of a weight to unit map to be used on the 'weight' column
        unit_map = {'kg': 1,'g': 0.001,'ml': 0.001, 'oz' : 35.274}

        for unit,value in unit_map.items():
        #Mask to search the df for all values that have a unit within them
            weights = df_working['weight'].str.contains(unit, na=False)
           
            if weights.any():
                #replacing the unit in the string with a blank which is stripped
                df_working.loc[weights, 'weight'] = df_working.loc[weights, 'weight'] .str.replace(unit, '', regex=False).str.strip() 
                #Checking if the value has an x which is then split and the two parts are multiplied together 
                df_working.loc[weights, 'weight'] = df_working.loc[weights, 'weight'].apply( lambda x: float(x.split("x")[0]) * float(x.split("x")[1]) if "x" in x else float(x)) * value
        
        df_working['weight'] = df_working['weight'].astype(float).round(2)
        
        return df_working 
    
    def clean_products_data(self,products_data):
        self.products_data = products_data
        
        #creating a copy of the df
        df_working = products_data.copy()
        
        #Removing random eroneus data that could interfere with weight cleaning. Clean being done on 'removed' column
        df_working['removed'] = df_working['removed'].astype(str) 
        regex = r'^(Removed|Still_avaliable)$'
        df_working.loc[~df_working['removed'].str.match(regex, na=False), 'removed'] = np.nan 
        df_working = df_working.dropna()   
        
        #Normalising the data by converting all weights into Kgs
        cleaned_df = self.convert_product_weight(df_working)
        
        return cleaned_df
    
    def clean_orders_data(self,orders_data):
        self.orders_data = orders_data
        #creating a copy of the df
        df_working = orders_data.copy()
        #Dropping first/last name, level 0 columns
        df_working.drop(columns=['first_name','last_name','1','level_0'],inplace=True)
        return df_working
    
    def clean_date_times_data(self,date_data):
        self.date_data = date_data
        #creating a copy of the df
        df_working = date_data.copy()
        #**Change "NULL" strings data type into NULL data type**
        df_working.replace(['NULL'],None, inplace=True)
        #Changing data type for multiple columns to int
        cols = ['month','year','day']
        df_working[cols] = df_working[cols].apply(pd.to_numeric ,errors='coerce', axis=1).astype('Int32')
        #Dropping all NULLs
        df_working.dropna(inplace=True)
        return df_working



                    






