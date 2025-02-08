from dateutil.parser import parse
import numpy as np
import pandas as pd


#Clean data from different sources
class DataCleaning:
    @staticmethod
    def remove_null_strings(df):
     '''
        Purpose:
            Clean dataframe by converting NULL strings to NULL dtype and then removing all NULLs 

        Arguments: 
            df: Dataframe to be cleaned 

        Returns:
            Cleaned dataframe with no NULL strings
        '''
     
     df.replace({'NULL': None}, inplace=True)
     df.dropna(axis=0, inplace=True,thresh=2)
     return df
    
    def clean_user_data(self,user_table):
        '''
        Purpose:
            Retrieve and clean the user data 

        Arguments: 
            user_table: Dataframe containing the user data

        Returns:
            Cleaned user data
        '''
        self.user_table = user_table
        #Creating a copy of the dataframe
        user_df = user_table.copy()
        #Change "NULL" strings data type into NULL data type and then removing
        user_df = self.remove_null_strings(user_df)
        #Finding additional NULL values by cleaning country_code
        country_code_regex = '^(?!.*\d)'
        try:
            user_df.loc[~user_df['country_code'].str.match(country_code_regex),'country_code'] = np.nan
        except TypeError:
            pass
        #Dropping NULL values
        user_df.dropna(axis=0, inplace=True)

        #Converting Join_date column to Datetime data type using parse
        try:
            user_df['join_date'] = user_df['join_date'].apply(parse)
        except TypeError:
            pass
        user_df['join_date'] = pd.to_datetime(user_df['join_date'],errors='coerce')

        #resetting index after cleaning process
        user_df.reset_index(drop=True)

        return user_df
    
    def clean_card_data(self,card_details):
        '''
        Purpose:
            Clean card details data
        
        Arguments: 
            card_details: Dataframe containing all card details
        Returns:
            Cleaned card details data 
        '''
        self.card_details = card_details
        #Creating a copy of the dataframe
        card_df = card_details.copy()
        #Change "NULL" strings data type into NULL data type and then removing
        card_df = self.remove_null_strings(card_df)
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
        '''
        Purpose:
            Clean store details data
        
        Arguments:
            store_data: Dataframe containing all store details 
        
        Returns:
            Cleaned store details data
        '''
        self.store_data = store_data 
        #Creating a copy of the dataframe
        store_df = store_data.copy()
        #Change "NULL" strings data type into NULL data type and then removing
        store_df = self.remove_null_strings(store_df)
    
        #Finding additional NULL values by cleaning country_code
        regex = '^(?!.*\d)'
        try:
            store_df.loc[~store_df['continent'].str.match(regex),'continent'] = np.nan
            store_df.loc[~store_df['store_type'].str.match(regex),'store_type'] = np.nan
        except TypeError:
            pass

        #Dropping NULL values while limiting threshold to ensure only needed rows are dropped
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
        try:
            store_df['staff_numbers'] = store_df['staff_numbers'].str.strip()
        except AttributeError:
            pass
        #Casting to int
        store_df['staff_numbers'] = store_df['staff_numbers'].astype('int64')

        return store_df
    

    def convert_product_weight(self,products_data):
        '''
        Purpose:
            Converts the 'weight' column from mixed weights to conform to float numbers representing their weight in KG.
            Uses a dictionary (unit_map) to establish the conversion needed depending on the units 

        Arguments: 
            products_data: A dataframe containing the products_data 
        return:
            A dataframe with the 'weight' column cleaned 
        '''
        self.products_data = products_data
        #Creating a copy of the dataframe
        df_working = products_data.copy()
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
        
        #Converting all weights into float with 2 decimal points
        df_working['weight'] = df_working['weight'].astype(float).round(2)
        
        return df_working 
    
    def clean_products_data(self,products_data):
        '''
        Purpose:
            Clean the products details data
        
        Arguments: 
            products_data: Dataframe containing all product details
        Returns:
             Cleaned products details data
        '''
        self.products_data = products_data
        
        #creating a copy of the df
        df_working = products_data.copy()
        
        #Removing random eroneus data that could interfere with weight cleaning. Clean being done on 'removed' column
        df_working['removed'] = df_working['removed'].astype(str) 
        regex = r'^(Removed|Still_avaliable)$'
        df_working.loc[~df_working['removed'].str.match(regex, na=False), 'removed'] = np.nan 
        df_working = df_working.dropna()   
        
        #Normalising the data by converting all weights into Kgs
        products_df = self.convert_product_weight(df_working)
        
        return products_df
    
    def clean_orders_data(self,orders_data):
        '''
        Purpose:
            Clean the order details data
        
        Arguments: 
            orders_data: Dataframe containing all order details
        Returns:
             Cleaned order details data
        '''
        self.orders_data = orders_data
        #creating a copy of the df
        orders_df = orders_data.copy()
        #Dropping first name,last name and level 0 columns
        orders_df.drop(columns=['first_name','last_name','1','level_0'],inplace=True)
        return orders_df
    
    def clean_date_times_data(self,date_data):
        '''
        Purpose:
            Clean the date/times details data
        
        Arguments: 
            date_data: Dataframe containing all date/time details
        Returns:
             Cleaned date/times details data
        '''
        self.date_data = date_data
        #creating a copy of the df
        date_time_df = date_data.copy()
        #**Change "NULL" strings data type into NULL data type**
        date_time_df.replace(['NULL'],None, inplace=True)
        #Changing data type for multiple columns to int
        cols = ['month','year','day']
        date_time_df[cols] = date_time_df[cols].apply(pd.to_numeric ,errors='coerce', axis=1).astype('Int32')
        #Dropping all NULLs
        date_time_df.dropna(inplace=True)
        return date_time_df



                    
if __name__ == '__main__':
    print('Please run mrdc_main.py instead, data_cleaning is used to provide functionality only ')





