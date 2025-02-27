# Multinational Retail Data Centralisation (MRDC)

## Description

This project focuses on the use of ETL (Extracting,Transforming and Loading). It is a traditionally accepted way  to combine data from multiple systems into a single database, used to aggregate data to analyze and drive business decisions.  
This project extracts data from a number of sources, these include:
- `API's`
- `AWS S3 buckets`
- `AWS RDS`
- Multiple file types, such as `JSON`,`CSV` and `PDF`

The purpose of this project is to extract information by collating all data from the above sources. Once done the data goes through the process of transformation through normalisation, cleaning and validation. Finally, data is loaded into a centralised database, 'PostgreSQL' where it can be used to query and drive critical business decision making.

Ultimately this project will improve business decision making by automating the data gathering and cleaning process. Not only will this increase efficiency but also provide the tools for comprehensive data analysis.

Below is a simple flow chart showing the overall process:

<img src="Images/MRDC_flow.png" width="1000" height="500">

## Installation Instructions
1. Clone the github repository:
   ``` bash 
   git clone https://github.com/YASamcaha/MRDC_Project.git
   ```
   ``` bash
   cd MRDC_project
   ```

2. Within the `credentials` folder provide the credentials needed:
   - API: This is the API key used to gather store data. Save the key and content type.
   - RDS Instance: This is the credentials to access the AWS RDS. Save the details as `db_instance_creds.yaml`
   - Local DB: This is the credentials for your local DB which in this case is a PostgreSQL database. Save the details as `local_db_creds.yaml`

3. Dependencies: Install required packages using the following command:
     ```
     pip install -r requirements.txt
     ```
4. Final set up:
   - Create a new database in `PostgreSQL` called `sales_data` which is where all the data will be loaded to.

## Usage Guide
1. The project is comprised of three main files that form the ETL process. Each file has a class that is used in the `mrdc_main` to perform the ETL process for the retail data. 
   - `database_utils` - This deals with connecting to the different databases and is vital in both the `extraction` and `loading` process.
   - `data_extraction` - This file is used to extract the data from the different sources and forms part of the `extraction` process.
   - `data_cleaning` - This file is used to clean the extracted data and prepare it for loading, this covers the `transformation` part of the process.

2. Once `mrdc_main` completes the data upload the next step is to configure the tables using PostgreSQL.
   To do this use the `sales_data_config` file, this will create the `database schema`. After this has completed the database schema will be similar to the below star schema:

   <img src="Images/MRDC_ERD.pgerd.png" width="700" height="700">

3. Finally, the database can now be used to query the data enabling business questions to be answered. 
   Please refer to `sales_data_queries.sql` for some examples of how the data can be used for the needs of the business.



## File Structure
***

      |   database_utils.py
      |   data_cleaning.py
      |   data_extraction.py
      |   mrdc_main.py
      |   requirements.txt
      |   sales_data_config.sql
      |   sales_data_queries.sql
      |
      +---credentials
      |       api_key.yaml
      |       db_instance_creds.yaml
      |       local_db_creds.yaml
      |
      +---Images
      |       MRDC_flow.png
      |       MRDC_ERD.pgerd.png



