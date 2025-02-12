
--How many stores does the business have and in which countries?
SELECT
    country_code AS Country,
    COUNT(locality) AS total_no_stores
FROM
    dim_store_details
GROUP BY
    country_code
ORDER BY
    total_no_stores DESC;


--The top 7 localities by store numbers
SELECT
    locality,
    COUNT(locality) AS total_no_stores
FROM
    dim_store_details
GROUP BY
    locality
ORDER BY
    total_no_stores DESC
LIMIT 7;




--Top 6 months for sales 
SELECT 
    SUM(orders_table.product_quantity * dim_products.product_price) AS total_sales,
    dim_date_times.month
FROM
    dim_products
JOIN
    orders_table ON dim_products.product_code = orders_table.product_code
JOIN
    dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid

GROUP BY
    dim_date_times.month
ORDER BY
    total_sales DESC
LIMIT
    6;




--How many sales are coming from online vs offline?
SELECT
    COUNT(product_code) AS number_of_sales,
    SUM(product_quantity) AS product_quantity_count,
    CASE
    WHEN dim_store_details.store_type = 'Web Portal' THEN 'Web'
    ELSE 'Offline'
    END AS location
FROM orders_table

JOIN
    dim_store_details ON orders_table.store_code = dim_store_details.store_code

GROUP BY
    location
ORDER BY
    number_of_sales DESC;


--What is the percentage of sales coming from each store type?
SELECT
    dim_store_details.store_type AS store_type,
    SUM(orders_table.product_quantity * dim_products.product_price) AS total_sales,
    ROUND(SUM(orders_table.product_quantity * dim_products.product_price)*100/SUM(SUM(orders_table.product_quantity * dim_products.product_price)) over (),2) as "sales_made(%)"
FROM
    orders_table
JOIN
    dim_store_details ON orders_table.store_code = dim_store_details.store_code
JOIN
    dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY
    store_type
ORDER BY
    total_sales DESC;



--Which month in each year produced the highest sales?
SELECT 
    SUM(orders_table.product_quantity * dim_products.product_price) AS total_sales,
    dim_date_times.year AS year,
    dim_date_times.month AS month
FROM
    dim_products

JOIN
    orders_table ON dim_products.product_code = orders_table.product_code
JOIN
    dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid

GROUP BY
    year,month
ORDER BY
    total_sales DESC
LIMIT 10;



--What is total staff headcount by country code?
SELECT
    SUM(staff_numbers) AS total_staff_numbers,
    country_code
    
FROM
    dim_store_details
GROUP BY
    country_code
ORDER BY    


--Which German store type is selling the most?
SELECT
    SUM(orders_table.product_quantity * dim_products.product_price) AS total_sales,
    store_type,
    country_code
FROM
    dim_store_details
JOIN
    orders_table ON dim_store_details.store_code = orders_table.store_code
JOIN
    dim_products ON orders_table.product_code = dim_products.product_code
WHERE
    country_code = 'DE'
GROUP BY
    store_type,country_code
ORDER BY
    total_sales DESC;




-- How quickly is the company making sales?
WITH
    timestamp_cte AS (SELECT
                        CAST(CONCAT(year, '-', month, '-', day, ' ', timestamp) AS TIMESTAMP) AS "timestamp",
                        year
                        FROM 
                        dim_date_times
                     ),
    lag_cte AS(SELECT
                    LEAD("timestamp") OVER (ORDER BY "timestamp" DESC) AS timing_difference,
                    "timestamp",
                    year
                FROM
                    timestamp_cte
                    )
SELECT 
    year,
    AVG("timestamp" - timing_difference) AS actual_time_taken
FROM
    lag_cte
GROUP BY
    year
ORDER BY 
    actual_time_taken DESC
LIMIT 5;

