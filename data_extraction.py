#Python script to extract data from different data sources: 
#CSV, API, and S3 bucket

class DataExtractor: 
	def __init__(self)

#import required modules
from database_utils.py import init_db_engine
from sqlalchemy import inspect
import pandas as pd

#function to list all the tables in the database
	def list_db_tables():
		#create an inspector
		inspector = inspect(engine)
		#Get schema names
		schema_names = inspector.get_schema_names()
		table_names = []
		#Add table names to a list and print
		for schema in schema_names:
			table_names += inspector.get_table_names(schema=schema)
		print(table_names)
		return table_names
#Function to extract a db table to a pandas df
	def read_rds_table(table_name):
		table_df = pd.read_sql_table(table_name, engine)
		table_df.head()
		return table_df

