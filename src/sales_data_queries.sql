/* Setting up and querying the sales_data database for multinational retail company. This douments the steps used to create the database and set up the connection scheme. Followed by several data queries */

/* #Step1: Recast columns of dim_orders_table to the correct data type. 
to uuid. NB Casting to UUID[] will cast to an array */

ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;

ALTER TABLE orders_table
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID;

-- The max number of characters indicated in brackets.  
ALTER TABLE orders_table
ALTER COLUMN card_number TYPE VARCHAR(19) USING card_number::VARCHAR(19);

ALTER TABLE orders_table
ALTER COLUMN store_code TYPE VARCHAR(11) USING store_code::VARCHAR(11);

ALTER TABLE orders_table
ALTER COLUMN product_code TYPE VARCHAR(11) USING product_code::VARCHAR(11);

ALTER TABLE orders_table
ALTER COLUMN product_quantity TYPE BIGINT USING product_quantity::SMALLINT;

/* Step 2: Cast the columns of dim_users_table to the correct data types */

ALTER TABLE dim_users_table
ALTER COLUMN first_name TYPE VARCHAR(255) USING first_name::VARCHAR(255);

ALTER TABLE dim_users_table
ALTER COLUMN last_name TYPE VARCHAR(255) USING last_name::VARCHAR(255);

ALTER TABLE dim_users_table
ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::DATE;

ALTER TABLE dim_users_table
ALTER COLUMN country_code TYPE VARCHAR(2) USING country_code::VARCHAR(2);

ALTER TABLE dim_users_table
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID;

ALTER TABLE dim_users_table
ALTER COLUMN join_date TYPE DATE USING join_date::DATE;

/* Step 3: dim_store_details table: merge latitude columns */
    /* Find the datatype of the latitude column */

SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
	WHERE table_name='dim_store_details' AND COLUMN_NAME='latitude';

    /* Create a column by concatenating lat and latitude */
ALTER TABLE dim_store_details
    ADD latitude_new VARCHAR;

UPDATE dim_store_details
	SET latitude_new = (CONCAT(latitude, lat));

    /* Drop latitude and lat from the table */
ALTER TABLE dim_store_details
    DROP COLUMN lat;

ALTER TABLE dim_store_details 
    DROP COLUMN latitude;

    /* Rename latitude_new to latitude */
ALTER TABLE dim_store_details
RENAME COLUMN latitude_new TO latitude; 

    /* CONCAT ignores NULL, so if there are two NULL cells this produces an empty cell.
    Replace the empty cells with NULL */
UPDATE dim_store_details
SET latitude = NULL WHERE latitude = ''; 


/* Step 4: set dim_store_details column data types */

ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE FLOAT USING longitude::FLOAT;

ALTER TABLE dim_store_details
ALTER COLUMN locality TYPE VARCHAR(255) USING locality::VARCHAR(255);

ALTER TABLE dim_store_details
ALTER COLUMN store_code TYPE VARCHAR(11) USING store_code::VARCHAR(11);

ALTER TABLE dim_store_details
ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::SMALLINT;

ALTER TABLE dim_store_details
ALTER COLUMN opening_date TYPE DATE USING opening_date::DATE;

ALTER TABLE dim_store_details
ALTER COLUMN store_type TYPE VARCHAR(255) USING store_type::VARCHAR(255); -- This needs to be nullable. Columns are nullable by default so I would only need to do something if null was NOT allowed. 

ALTER TABLE dim_store_details
ALTER COLUMN latitude TYPE FLOAT USING latitude::FLOAT;

ALTER TABLE dim_store_details
ALTER COLUMN country_code TYPE VARCHAR(2) USING country_code::VARCHAR(2);

ALTER TABLE dim_store_details
ALTER COLUMN continent TYPE VARCHAR(255) USING continent::VARCHAR(255);

/* Step 5: Change the row in the location column in dim_store_details that is null to na 
This row has already been removed in a prior cleaning step before this was uploaded to the db
But the code that could be used is below: */

/* Check if there are any rows containing certain characters*/
SELECT * from  dim_store_details
WHERE locality IS NULL; 

/* OR */

SELECT locality 
FROM dim_store_details
WHERE locality like '%nu%';

/*Replace NULL with NA*/

UPDATE dim_store_details
SET locality = REPLACE(locality, 'NULL', 'N/A'); 

/* Task 4 (dim_products): Step 6: Remove the £ from the product_price column in the dim_products table */

--Check which lines contain '£'
    SELECT product_price
    FROM dim_products
    WHERE product_price like '%£%';

    --Replace '£' with ''
    UPDATE dim_products
    SET product_price = REPLACE(product_price, '£', ''); 

/* Step 7: Add a new human readable weight_class column that'll contain catagories based on the weight range of the products */

ALTER TABLE dim_products
    ADD weight_class TEXT; 

UPDATE dim_products
SET weight_class = CASE 
					WHEN final_weight <2 THEN 'Light'
					WHEN  final_weight >=2 AND final_weight <40 THEN 'Mid_Sized'
					WHEN final_weight >=40 AND final_weight <140 THEN 'Heavy'
					WHEN final_weight >= 140 THEN 'Truck_Required'
					END; 

ALTER TABLE dim_products
ALTER COLUMN weight_class TYPE VARCHAR(14) USING weight_class::VARCHAR(14);

/* Task 5: dim_products 
Step 8: rename columns 'removed' and 'final_weight' */

ALTER TABLE dim_products
RENAME COLUMN removed TO still_available; 


/* Step 9: recast data types of columns */

ALTER TABLE dim_products
ALTER COLUMN product_price TYPE FLOAT USING product_price::FLOAT;


--determine max length of EAN column. EAN column name is capatilised so needs to be in quotes

SELECT "EAN", LENGTH("EAN") 
FROM dim_products
ORDER BY LENGTH("EAN") DESC;

ALTER TABLE dim_products
ALTER COLUMN "EAN" TYPE VARCHAR(17) USING "EAN"::VARCHAR(17);

ALTER TABLE dim_products
ALTER COLUMN product_code TYPE VARCHAR(11) USING product_code::VARCHAR(11); --length determined in dim_orders_table

ALTER TABLE dim_products
ALTER COLUMN date_added TYPE DATE USING date_added::DATE;

ALTER TABLE dim_products
ALTER COLUMN uuid TYPE UUID USING uuid::UUID;

ALTER TABLE dim_products
ALTER COLUMN weight_class TYPE VARCHAR(14) USING weight_class::VARCHAR(14);

--Convert to boolean
ALTER TABLE dim_products
ADD bool BOOLEAN;

UPDATE dim_products
SET bool = CASE 
			WHEN still_available = 'Still_avaliable' THEN True
			WHEN still_available = 'Removed' THEN False
			END; 

--Remove still_available column and rename bool to still_available
ALTER TABLE dim_products
DROP still_available;

ALTER TABLE dim_products
RENAME COLUMN bool TO still_available; 

/* Task 6: dim_date_times 
Step 10: update column data types */

ALTER TABLE dim_date_times
ALTER COLUMN month TYPE VARCHAR(2) USING month::VARCHAR(2);

ALTER TABLE dim_date_times
ALTER COLUMN year TYPE VARCHAR(4) USING year::VARCHAR(4);

ALTER TABLE dim_date_times
ALTER COLUMN day TYPE VARCHAR(2) USING day::VARCHAR(2);

ALTER TABLE dim_date_times
ALTER COLUMN time_period TYPE VARCHAR(10) USING time_period::VARCHAR(10);

ALTER TABLE dim_date_times
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;

/* Task 7: dim_card_details
Step 11: update column data types */

ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(19) USING card_number::VARCHAR(19); --length determined while editing orders_table

ALTER TABLE dim_card_details
ALTER COLUMN expiry_date TYPE VARCHAR(5) USING expiry_date::VARCHAR(5); --determined by inspecting the table in postgres

ALTER TABLE dim_card_details
ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::DATE;


/* Task 8: Add primary keys to all dim_ tables. The primary key column in each table matches a column in orders_table*/
ALTER TABLE dim_date_times
ADD PRIMARY KEY (date_uuid);

ALTER TABLE dim_card_details
ADD PRIMARY KEY (card_number);

ALTER TABLE dim_users_table
ADD PRIMARY KEY (user_uuid);

ALTER TABLE dim_store_details
ADD PRIMARY KEY (store_code);

ALTER TABLE dim_products
ADD PRIMARY KEY (product_code);

/* Task 9: Add foreign key to orders_table */

ALTER TABLE orders_table   
	ADD CONSTRAINT fk_card_number 
		FOREIGN KEY (card_number) 
			REFERENCES dim_card_details (card_number); 

ALTER TABLE orders_table  
	ADD CONSTRAINT fk_date_uuid 
		FOREIGN KEY (date_uuid) 
			REFERENCES dim_date_times (date_uuid);
			
ALTER TABLE orders_table  
	ADD CONSTRAINT fk_product_code 
		FOREIGN KEY (product_code) 
			REFERENCES dim_products (product_code); 

ALTER TABLE orders_table  
	ADD CONSTRAINT fk_store_code 
		FOREIGN KEY (store_code) 
			REFERENCES dim_store_details (store_code); 

ALTER TABLE orders_table  
	ADD CONSTRAINT fk_user_uuid 
		FOREIGN KEY (user_uuid) 
			REFERENCES dim_users_table (user_uuid); 

