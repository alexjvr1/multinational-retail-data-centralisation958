from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

#Initiate instances of each Class
###credentials = path to yaml file that contains the database credentials
dc = DatabaseConnector(credentials="../db_creds.yaml")
de = DataExtractor()
dcln = DataCleaning()

#Step1: read the database credentials and return a db engine that can be initialised later
engine=dc.init_db_creds()
print(engine)

#Step2: list all the tables in the RDS database
table_names = de.list_db_tables(engine=engine)
print(table_names)

#Step3: extract the user table to a pandas dataframe
users_table = de.read_rds_table(engine=engine, table_name="legacy_users")
users_table.info()

#Step4: clean the user table
user_table_cleaned = dcln.clean_user_data(users_table)

#Step5: Upload to SQL database
dc.upload_to_db(user_table_cleaned, sql_table_name="dim_users_table")


#Step6: Extract all the user card details from a link to a pdf and return a pd.df
pdf_path = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
card_details = de.retrieve_pdf_data(pdf_path=pdf_path)
card_details.info()

#Step7: Clean card details
card_details_cleaned = dcln.clean_card_data(card_details)

#Step8: Upload card details to sql database
dc.upload_to_db(card_details_cleaned, sql_table_name="dim_card_details")

#Step9: Retrieve the store information from an API
    #9.1: Define a header dictionary
api_header_dict = {"x-api-key":"yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}

    #9.2: Define the end points to return the number of stores
number_of_stores_endpoint="https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"

    #9.3: Retrieve the number of stores
number_of_stores=de.list_number_of_stores(number_of_stores_endpoint, api_header_dict)
print("Total number of stores:"+str(number_of_stores))

    #9.4: Define the end point for the store date by specifying the number of stores
retrieve_stores_endpoint="https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/"

    #9.4: Loop through urls for each store to retrieve store information and return a pandas df
store_data=de.retrieve_stores_data(retrieve_stores_endpoint, api_header_dict, number_of_stores)
store_data.info()

#Step 10: Clean store information
store_data_cleaned = dcln.clean_store_data(store_data)
store_data_cleaned.info()

#Step 11: Upload store information to sql database
dc.upload_to_db(store_data_cleaned, sql_table_name="dim_store_details")


#Step 12: Extract product details from S3 bucket
product_details = de.extract_from_s3("s3://data-handling-public/products.csv")
product_details.info()

#Step 13: Change all weights to kg in product details df
product_details_kg = dcln.convert_product_weights(product_details)

#Step 14: Clean product_details df
product_details_cleaned = dcln.clean_products_data(product_details_kg)

#Step 15: Upload product details df to SQL datbase
dc.upload_to_db(product_details_cleaned, sql_table_name="dim_products")

#Step 16: List all available data tables to get the name of the orders table
engine=dc.init_db_creds()
table_names = de.list_db_tables(engine)
print(table_names)

#Step 17: Extract the orders table to a pandas df
orders_table = de.read_rds_table("orders_table", engine)

#Step 18: Clean orders_table
orders_table_cleaned = dcln.clean_orders_table(orders_table)

#Step 19: Upload the orders table to the sql database
dc.upload_to_db(orders_table_cleaned, sql_table_name="orders_table")

#Step 20: Extract the details of when each sale occurred and return a pandas dataframe: 
s3_address = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
date_details = de.extract_json_from_s3(s3_address)

date_details.info()


#Step 21: Clean date_details
date_details_cleaned = dcln.clean_date_details(date_details)


#Step 22: Upload the date_details table to the sql database
dc.upload_to_db(date_details_cleaned, sql_table_name="dim_date_times")



#def execute_main():


if __name__ == "__main__":  # Condition to ensure module is executed not imported. 
    execute_main()