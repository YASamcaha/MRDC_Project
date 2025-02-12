
--Casting data types for orders_table
-- Finding Max length of multiple columns
SELECT 
	MAX(LENGTH(card_number::text)) AS card_number_max,
    MAX(LENGTH(store_code::text)) AS store_code_max,
    MAX(LENGTH(product_code::text)) AS product_code_max
FROM 
	orders_table;

-- Casting columns to their correct data types
ALTER TABLE orders_table
    ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID,
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
    AlTER COLUMN product_quantity TYPE SMALLINT,
--Using result of Max length to assign VARCHAR
    ALTER COLUMN card_number TYPE VARCHAR(19),
    ALTER COLUMN store_code TYPE VARCHAR(12),
    ALTER COLUMN product_code TYPE VARCHAR(11);

--Casting data types for dim_users
SELECT 
	MAX(LENGTH(country_code::text)) AS country_code_max
FROM 
	dim_users;



ALTER TABLE dim_users
    ALTER COLUMN first_name TYPE VARCHAR(255),
    ALTER COLUMN last_name TYPE VARCHAR(255),
    ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::DATE,
--Using result of Max length to assign VARCHAR
    ALTER COLUMN country_code TYPE VARCHAR(3),
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
    ALTER COLUMN join_date TYPE DATE USING join_date::DATE;


--Casting data types for dim_store_details

BEGIN;
--Combining Latitude columns
UPDATE dim_store_details
    SET Latitude = CONCAT(latitude,lat);

--Dropping lat column 
ALTER TABLE dim_store_details
    DROP COLUMN lat CASCADE;

--Finding Max length of VARCHAR columns
SELECT 
	MAX(LENGTH(store_code::text)) AS store_code_max,
    MAX(LENGTH(country_code::text)) AS country_code_max
FROM 
	dim_store_details;


UPDATE dim_store_details
    SET longitude = NULL
    WHERE longitude = 'N/A';

UPDATE dim_store_details
    SET latitude = NULL
    WHERE latitude = 'N/A';

ALTER TABLE dim_store_details
    ALTER COLUMN longitude TYPE NUMERIC USING longitude::DECIMAL,
    ALTER COLUMN locality TYPE VARCHAR(255),
    ALTER COLUMN store_code TYPE VARCHAR(12),
    ALTER COLUMN staff_numbers TYPE SMALLINT,
    ALTER COLUMN opening_date TYPE DATE USING opening_date::DATE,
    ALTER COLUMN store_type TYPE VARCHAR(255),
    ALTER COLUMN latitude TYPE NUMERIC USING latitude::DECIMAL,
    ALTER COLUMN country_code TYPE VARCHAR(2),
    ALTER COLUMN continent TYPE VARCHAR(255);

COMMIT;

--Casting data types for dim_products

BEGIN;
--Removing the £ sign from product_price
UPDATE dim_products
    SET product_price = TRIM('£' FROM product_price);

--Creation of weight_class column and adding human readable values
ALTER TABLE dim_products
    ADD COLUMN weight_class TEXT;

UPDATE dim_products
    SET weight_class = (CASE
                            WHEN weight < 2 THEN 'Light'
                            WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
                            WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
                            ELSE 'Truck_Required'
                            END);


--Renaming removed column
ALTER TABLE dim_products
    RENAME removed TO still_available;
--Changing values to support BOOL data type change    
UPDATE dim_products
    SET still_available = (CASE
                             WHEN still_available = 'Removed' THEN 'False'
                             ELSE 'True'
                             END);

--Finding Max length of VARCHAR columns
SELECT 
	MAX(LENGTH("EAN"::text)) AS EAN_max,
    MAX(LENGTH(product_code::text)) AS product_code_max,
    MAX(LENGTH(weight_class::text)) AS weight_class_max
FROM 
	dim_products;

    
    
ALTER TABLE dim_products  
    ALTER COLUMN product_price TYPE NUMERIC USING product_price::DECIMAL,
    ALTER COLUMN weight TYPE NUMERIC,
    ALTER COLUMN "EAN" TYPE VARCHAR(17),
    ALTER COLUMN product_code TYPE VARCHAR(11),
    ALTER COLUMN date_added TYPE DATE USING date_added::DATE,
    ALTER COLUMN uuid TYPE UUID USING uuid::UUID,
    ALTER COLUMN still_available TYPE BOOL USING still_available::BOOL,
    ALTER COLUMN weight_class TYPE VARCHAR(14);

COMMIT;

--Casting data types for dim_date_times

BEGIN;

--Finding Max length of VARCHAR columns
SELECT 
	MAX(LENGTH("month"::text)) AS month_max,
    MAX(LENGTH("year"::text)) AS year_max,
    MAX(LENGTH("day"::text)) AS day_max,
    MAX(LENGTH(time_period::text)) AS time_period_max
FROM 
	dim_date_times;

ALTER TABLE dim_date_times
    ALTER COLUMN "month" TYPE VARCHAR(2),
    ALTER COLUMN "year" TYPE VARCHAR(4),
    ALTER COLUMN "day" TYPE VARCHAR(2),
    ALTER COLUMN time_period TYPE VARCHAR(10),
    ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;

COMMIT;

--Casting data types for dim_card_details

BEGIN;

--Finding Max length of VARCHAR columns
SELECT 
	MAX(LENGTH(card_number::text)) AS card_number_max,
    MAX(LENGTH(expiry_date::text)) AS expiry_date_max
FROM 
	dim_card_details;

ALTER TABLE dim_card_details
    ALTER COLUMN card_number TYPE VARCHAR(19),
    ALTER COLUMN expiry_date TYPE VARCHAR(5),
    ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::DATE;

COMMIT;

--Updating dimension tables to have primary keys

BEGIN;
ALTER TABLE dim_card_details
    ADD PRIMARY KEY(card_number);

ALTER TABLE dim_date_times
    ADD PRIMARY KEY(date_uuid);

ALTER TABLE dim_products
    ADD PRIMARY KEY(product_code);

ALTER TABLE dim_users
    ADD PRIMARY KEY(user_uuid);

ALTER TABLE dim_store_details
    ADD PRIMARY KEY(store_code);

COMMIT;

--Updating fact table(orders_table) to have corresponding foreign keys

BEGIN;
ALTER TABLE orders_table
    ADD FOREIGN KEY(card_number) REFERENCES dim_card_details(card_number),
    ADD FOREIGN KEY(date_uuid) REFERENCES dim_date_times(date_uuid),
    ADD FOREIGN KEY(product_code) REFERENCES dim_products(product_code),
    ADD FOREIGN KEY(user_uuid) REFERENCES dim_users(user_uuid),
    ADD FOREIGN KEY(store_code) REFERENCES dim_store_details(store_code);
COMMIT;

