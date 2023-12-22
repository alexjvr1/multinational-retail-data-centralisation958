/* Setting up and querying the sales_data database for multinational retail company. This douments the steps used to create the database and set up the connection scheme. Followed by several data queries */

/* #Step1: Recast columns of dim_orders_table to the correct data type. 
to uuid. NB Casting to UUID[] will cast to an array */

ALTER TABLE dim_orders_table
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;

ALTER TABLE dim_orders_table
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID;

-- The max number of characters indicated in brackets.  
ALTER TABLE dim_orders_table
ALTER COLUMN card_number TYPE VARCHAR(19) USING card_number::VARCHAR(19);

ALTER TABLE dim_orders_table
ALTER COLUMN store_code TYPE VARCHAR(11) USING store_code::VARCHAR(11);

ALTER TABLE dim_orders_table
ALTER COLUMN product_code TYPE VARCHAR(11) USING product_code::VARCHAR(11);

ALTER TABLE dim_orders_table
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

/* Step 3: dim_store_details table: merge latitude columns


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
ALTER COLUMN store_type TYPE VARCHAR(255) NULL USING store_type::VARCHAR(255) NULL;

ALTER TABLE dim_store_details
ALTER COLUMN latitude TYPE FLOAT USING latitude::FLOAT;

ALTER TABLE dim_store_details
ALTER COLUMN country_code TYPE VARCHAR(2) USING country_code::VARCHAR(2);

ALTER TABLE dim_store_details
ALTER COLUMN continent TYPE VARCHAR(255) USING continent::VARCHAR(255);
